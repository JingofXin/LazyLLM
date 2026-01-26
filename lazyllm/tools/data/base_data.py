import functools
import os
import json
import pickle
import time
import concurrent.futures
import multiprocessing
import sys
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from tqdm import tqdm

from lazyllm import warp, LOG, config

config.add(
    'data_process_path', str, '', 'DATA_PROCESS_PATH',
    description='The path to store data files.')
config.add(
    'data_process_resume', bool, False, 'DATA_PROCESS_RESUME',
    description='Whether to resume data processing from saved state.')

class RegistryFactory:
    def __init__(self, wrapper=None):
        self._registry = {}
        self._tags = {}
        self._wrapper = wrapper

    def __getattr__(self, name):
        if name in self._registry:
            return self._registry[name]
        raise AttributeError(f'"{self.__class__.__name__}" has no attribute "{name}"')

    def register(self, obj=None, tag='default', **kwargs):
        if obj is None:
            # Support @register and @register(param=value) usages
            return functools.partial(self.register, tag=tag, **kwargs)

        if obj.__name__ in self._registry:
            raise ValueError(f'{obj.__name__} is already registered.')

        if self._wrapper:
            Wrapper = self._wrapper(obj, **kwargs)
        else:
            Wrapper = obj
        self._registry[obj.__name__] = Wrapper
        self._tags.setdefault(tag, []).append(obj.__name__)
        return Wrapper

class DataStateStore:
    def __init__(self, func_name, save_data=True, save_folder=None):
        self.save_data = save_data
        self.resume = config['data_process_resume']
        self.save_path = None
        self.progress_path = None
        self.func_name = func_name

        if self.save_data:
            # 允许外部传入共享目录前缀
            root = config['data_process_path'] or os.path.join(os.getcwd(), 'data_pipeline_res')
            base_folder = save_folder or root
            save_folder = os.path.join(base_folder, func_name)
            os.makedirs(save_folder, exist_ok=True)
            self.save_path = os.path.join(save_folder, f'{func_name}_results.jsonl')
            self.progress_path = f'{self.save_path}.pkl'
            self._init_files()

    def _init_files(self):
        if self.save_path and not self.resume:
            if os.path.exists(self.save_path):
                os.remove(self.save_path)
            if os.path.exists(self.progress_path):
                os.remove(self.progress_path)

    def load_progress(self):
        if self.save_path and self.resume and os.path.exists(self.progress_path):
            try:
                with open(self.progress_path, 'rb') as f:
                    return pickle.load(f)
            except Exception:
                LOG.warning(f'Failed to load progress from {self.progress_path}')
        return set()

    def save_results(self, results):
        if not self.save_path or not results:
            return
        with open(self.save_path, 'a', encoding='utf-8') as f:
            for res in results:
                try:
                    line = json.dumps(res, ensure_ascii=False)
                except Exception:
                    line = str(res)
                f.write(line + '\n')

    def save_progress(self, indices):
        if not self.save_path:
            return
        with open(self.progress_path, 'wb') as f:
            pickle.dump(indices, f)

    def load_results(self):
        results = []
        if self.save_path and os.path.exists(self.save_path):
            with open(self.save_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        results.append(json.loads(line))
                    except Exception:
                        pass
        return results

    def set_save_path(self, path):
        if not path: return
        self.save_path = path if path.endswith('.jsonl') else os.path.join(path, f'{self.func_name}_results.jsonl')
        os.makedirs(os.path.dirname(self.save_path), exist_ok=True)
        self.progress_path = f'{self.save_path}.pkl'
        self._init_files()

class _ComposedFunc:
    def __init__(self, operators):
        self.operators = operators
    
    def __call__(self, data, *args, **kwargs):
        for op in self.operators:
            if hasattr(op, '_run_one'):
                data = op._run_one(data)
            elif callable(op):
                data = op(data)
            
            if data is None:
                return None
        return data

class DataOp:
    def __init__(
            self, func, *args, _one_item=True, _max_workers=None, _batch_size=None, _concurrency=None, 
            _ignore_errors=False, _save_data=True, _state_store=None, **kwargs):
        
        self._func = func
        self._one_item = _one_item
        self._args = args
        self._kwargs = kwargs
        
        # Parse concurrency config
        self._concurrency_config = _concurrency or {}
        self._concurrency_type = self._concurrency_config.get('type', 'process')
        self._max_workers = _max_workers or self._concurrency_config.get('max_workers', os.cpu_count())
        self._batch_size = _batch_size or self._max_workers
        
        # _state_store handling
        shared_folder = None
        shared_store = None
        if isinstance(_state_store, DataStateStore):
            shared_store = _state_store
        elif isinstance(_state_store, dict):
            shared_folder = _state_store.get('save_folder')
        
        name = getattr(func, '__name__', 'AnonymousFunc')
        self._store = shared_store or DataStateStore(name, _save_data, save_folder=shared_folder)
        self._ignore_errors = _ignore_errors
        
        # Instantiate if it's a class
        self._obj = None
        if isinstance(func, type):
            try:
                self._obj = func(*args, **kwargs)
            except Exception:
                self._obj = func(*args, **kwargs)

    def set_output(self, path):
        self._store.set_save_path(path)
        return self

    def _run_one(self, data):
        try:
            if self._obj:
                return self._obj(data)
            return self._func(data, *self._args, **self._kwargs)
        except Exception as e:
            if self._ignore_errors:
                name = getattr(self._func, '__name__', 'func')
                LOG.error(f'Error processing data in {name}: {e}')
                return None
            raise e

    def _execute_stream_mode(self, data, executor_cls):
         func_name = getattr(self._func, '__name__', 'func')
         processed_indices = self._store.load_progress()
         final_results = []
         if not self._store.save_data:
             processed_indices = set()
         
         pending_indices = [i for i in range(len(data)) if i not in processed_indices]
         if not pending_indices:
             if self._store.save_data: return self._store.load_results()
             return []
        
         pbar = tqdm(total=len(data), desc=f'Processing {func_name}', unit='item')
         pbar.update(len(data) - len(pending_indices))
         
         batch_buffer = []

         with executor_cls(max_workers=self._max_workers) as executor:
            future_to_idx = {}
            pending_iter = iter(pending_indices)
            
            for _ in range(self._max_workers):
                try:
                    idx = next(pending_iter)
                    f = executor.submit(self._run_one, data[idx])
                    future_to_idx[f] = idx
                except StopIteration:
                    break
            
            while future_to_idx:
                done, _ = concurrent.futures.wait(future_to_idx.keys(), return_when=concurrent.futures.FIRST_COMPLETED)
                for f in done:
                    idx = future_to_idx.pop(f)
                    try:
                        res = f.result()
                        item_res = []
                        if res is None: pass
                        elif isinstance(res, list): item_res = res
                        else: item_res = [res]
                        
                        batch_buffer.extend(item_res)
                        if not self._store.save_data:
                            final_results.extend(item_res)
                        
                        processed_indices.add(idx)
                    except Exception as e:
                        if not self._ignore_errors: raise e
                        LOG.error(f'Error: {e}')
                    
                    pbar.update(1)
                    
                    try:
                        nxt = next(pending_iter)
                        nf = executor.submit(self._run_one, data[nxt])
                        future_to_idx[nf] = nxt
                    except StopIteration:
                        pass
                
                if self._store.save_data and len(batch_buffer) >= self._batch_size:
                    self._store.save_results(batch_buffer)
                    self._store.save_progress(processed_indices)
                    batch_buffer = []

         if self._store.save_data and batch_buffer:
             self._store.save_results(batch_buffer)
             self._store.save_progress(processed_indices)
         pbar.close()
         if self._store.save_data: return self._store.load_results()
         return final_results

    def __call__(self, data):
        if not self._one_item:
            if self._store.save_data and self._store.resume and 'Done' in self._store.load_progress():
                 return self._store.load_results()
            res = self._run_one(data)
            if self._store.save_data:
                self._store.save_results(res if res else [])
                self._store.save_progress('Done')
            return res

        assert isinstance(data, list)
        
        if self._concurrency_type == 'thread':
            return self._execute_stream_mode(data, ThreadPoolExecutor)
        elif self._concurrency_type == 'process':
            return self._execute_stream_mode(data, ProcessPoolExecutor)
        else:
            return self._process_common(data, 1, lambda batch: [self._run_one(item) for item in batch])

    def _process_common(self, data, batch_size, process_func):
        func_name = getattr(self._func, '__name__', 'func')
        processed_indices = self._store.load_progress()
        results = []
        if not self._store.save_data: processed_indices = set()
        
        pbar = tqdm(total=len(data), desc=f'Processing {func_name}', unit='item')
        pbar.update(len(processed_indices))

        for i in range(0, len(data), batch_size):
            batch_indices = list(range(i, min(i + batch_size, len(data))))
            pending_indices = [idx for idx in batch_indices if idx not in processed_indices]

            if not pending_indices:
                pbar.update(len(batch_indices))
                continue

            batch_data = [data[idx] for idx in pending_indices]
            batch_res = process_func(batch_data)

            filtered_batch_res = []
            for res in batch_res:
                if res is None:
                    continue
                if isinstance(res, list):
                    filtered_batch_res.extend(res)
                else:
                    filtered_batch_res.append(res)

            if self._store.save_data:
                self._store.save_results(filtered_batch_res)
                processed_indices.update(pending_indices)
                self._store.save_progress(processed_indices)
            else:
                results.extend(filtered_batch_res)

            pbar.update(len(batch_indices))

        pbar.close()
        if self._store.save_data:
            return self._store.load_results()
        return results

    def __repr__(self):
        return f'<DataOpWrapper for {getattr(self._func, "__name__", "func")}>'

class ComposeDataOperator:
    def __new__(cls, *operators, **kwargs):
        # Create the callable object
        composed = _ComposedFunc(operators)
        
        # Name it
        # Fixed: access _func instead of _obj as operators are DataOp instances where _obj might be None (if wrapping func)
        name_parts = [op._func.__name__ if hasattr(op, '_func') else getattr(op, '__name__', 'Op') for op in operators]
        composed.__name__ = f"Compose_{'_'.join(name_parts)}"

        # Wrap it. 
        return data_op_wrapper(composed, one_item=True)(**kwargs)

def data_op_wrapper(func, one_item=True):  # noqa: C901
    # Fix for pickling decorated classes in multiprocessing
    if hasattr(func, '__qualname__') and hasattr(sys, 'modules'):
        module_name = getattr(func, '__module__', None)
        if module_name and module_name in sys.modules:
            module = sys.modules[module_name]
            # Create a unique alias for the original class/function which will stay unwrapped
            # This allows pickle to find the original class definition during multiprocessing
            orig_name = f'_lazyllm_orig_{func.__name__}'
            if not hasattr(module, orig_name):
                setattr(module, orig_name, func)
                try:
                    # Modify qualname to match the alias so pickle lookup succeeds
                    func.__qualname__ = orig_name
                except Exception:
                    pass
    
    return functools.partial(DataOp, func, _one_item=one_item)

DataOperatorRegistry = RegistryFactory(data_op_wrapper)
