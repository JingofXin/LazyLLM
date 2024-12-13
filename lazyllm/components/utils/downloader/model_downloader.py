import os
import time
import tqdm
import shutil
import threading

import lazyllm
from .model_mapping import model_name_mapping, model_provider, model_groups
from lazyllm.common.common import EnvVarContextManager

lazyllm.config.add('model_source', str, 'modelscope', 'MODEL_SOURCE')
lazyllm.config.add('model_cache_dir', str, os.path.join(os.path.expanduser('~'), '.lazyllm', 'model'),
                   'MODEL_CACHE_DIR')
lazyllm.config.add('model_path', str, '', 'MODEL_PATH')
lazyllm.config.add('model_source_token', str, '', 'MODEL_SOURCE_TOKEN')
lazyllm.config.add('data_path', str, '', 'DATA_PATH')


class ModelManager():
    def __init__(self, model_source=lazyllm.config['model_source'],
                 token=lazyllm.config['model_source_token'],
                 cache_dir=lazyllm.config['model_cache_dir'],
                 model_path=lazyllm.config['model_path']):
        self.model_source = model_source
        self.token = token
        self.cache_dir = cache_dir
        self.model_paths = model_path.split(":") if len(model_path) > 0 else []
        self.progress = ProgressTracker()

    @classmethod
    def get_model_type(cls, model) -> str:
        assert isinstance(model, str) and len(model) > 0, "model name should be a non-empty string"
        for name, info in model_name_mapping.items():
            if 'type' not in info: continue

            model_name_set = {name.casefold()}
            for source in info['source']:
                model_name_set.add(info['source'][source].split('/')[-1].casefold())

            if model.split(os.sep)[-1].casefold() in model_name_set:
                return info['type']
        return 'llm'

    @classmethod
    def get_model_name(cls, model) -> str:
        search_string = os.path.basename(model)
        for model_name, sources in model_name_mapping.items():
            if model_name.lower() == search_string.lower() or any(
                os.path.basename(source_file).lower() == search_string.lower()
                for source_file in sources["source"].values()
            ):
                return model_name
        return ""

    @classmethod
    def get_model_prompt_keys(cls, model) -> dict:
        model_name = cls.get_model_name(model)
        if model_name and "prompt_keys" in model_name_mapping[model_name.lower()]:
            return model_name_mapping[model_name.lower()]["prompt_keys"]
        else:
            return dict()

    def _try_add_mapping(self, model):
        model_base = os.path.basename(model)
        model = model_base.lower()
        if model in model_name_mapping.keys():
            return
        matched_model_prefix = next((key for key in model_provider if model.startswith(key)), None)
        if matched_model_prefix and self.model_source in model_provider[matched_model_prefix]:
            matching_keys = [key for key in model_groups.keys() if key in model]
            if matching_keys:
                matched_groups = max(matching_keys, key=len)
                model_name_mapping[model] = {
                    "prompt_keys": model_groups[matched_groups]["prompt_keys"],
                    "source": {k: v + '/' + model_base for k, v in model_provider[matched_model_prefix].items()}
                }

    def download(self, model='', call_back=None):
        assert isinstance(model, str), "model name should be a string."
        self._try_add_mapping(model)
        # Dummy or local model.
        if len(model) == 0 or model[0] in (os.sep, '.', '~') or os.path.isabs(model): return model

        model_at_path = self._model_exists_at_path(model)
        if model_at_path: return model_at_path

        if self.model_source == '' or self.model_source not in ('huggingface', 'modelscope'):
            print("[WARNING] model automatic downloads only support Huggingface and Modelscope currently.")
            return model

        if call_back:
            self.progress.set_callback(call_back)

        if model.lower() in model_name_mapping.keys() and \
                self.model_source in model_name_mapping[model.lower()]['source'].keys():
            full_model_dir = os.path.join(self.cache_dir, model)

            mapped_model_name = model_name_mapping[model.lower()]['source'][self.model_source]
            model_save_dir = self._do_download(mapped_model_name)
            if model_save_dir:
                # The code safely creates a symbolic link by removing any existing target.
                if os.path.exists(full_model_dir):
                    os.remove(full_model_dir)
                if os.path.islink(full_model_dir):
                    os.unlink(full_model_dir)
                os.symlink(model_save_dir, full_model_dir, target_is_directory=True)
                return full_model_dir
            return model  # failed to download model, keep model as it is
        else:
            model_name_for_download = model

            # Try to figure out a possible model provider
            matched_model_prefix = next((key for key in model_provider if model.lower().startswith(key)), None)
            if matched_model_prefix and self.model_source in model_provider[matched_model_prefix]:
                model_name_for_download = model_provider[matched_model_prefix][self.model_source] + '/' + model

            model_save_dir = self._do_download(model_name_for_download)
            return model_save_dir if model_save_dir else model

    def _model_exists_at_path(self, model_name):
        if len(self.model_paths) == 0:
            return None
        model_dirs = []

        # For short model name, get all possible names from the mapping.
        if model_name.lower() in model_name_mapping.keys():
            for source in ('huggingface', 'modelscope'):
                if source in model_name_mapping[model_name.lower()]['source'].keys():
                    model_dirs.append(model_name_mapping[model_name.lower()]['source'][source].replace('/', os.sep))
        model_dirs.append(model_name.replace('/', os.sep))

        for model_path in self.model_paths:
            if len(model_path) == 0: continue
            if model_path[0] != os.sep:
                print(f"[WARNING] skipping path {model_path} as only absolute paths is accepted.")
                continue
            for model_dir in model_dirs:
                full_model_dir = os.path.join(model_path, model_dir)
                if self._is_model_valid(full_model_dir):
                    return full_model_dir
        return None

    def _is_model_valid(self, model_dir):
        if not os.path.isdir(model_dir):
            return False
        return any((True for _ in os.scandir(model_dir)))

    def _do_download(self, model=''):
        model_dir = model.replace('/', os.sep)
        full_model_dir = os.path.join(self.cache_dir, self.model_source, model_dir)

        try:
            if self.model_source == 'huggingface':
                return self._download_model_from_hf(model, full_model_dir)
            elif self.model_source == 'modelscope':
                return self._download_model_from_ms(model, full_model_dir)
        # Use `BaseException` to capture `KeyboardInterrupt` and normal `Exceptioin`.
        except BaseException as e:
            lazyllm.LOG.warning(f"Huggingface: {e}")
            if not self.token:
                lazyllm.LOG.warning('Token is empty, which may prevent private models from being downloaded, '
                                    'as indicated by "the model does not exist." Please set the token with the '
                                    'environment variable LAZYLLM_MODEL_SOURCE_TOKEN to download private models.')
            if os.path.isdir(full_model_dir):
                shutil.rmtree(full_model_dir)
                lazyllm.LOG.warning(f"{full_model_dir} removed due to exceptions.")
        return model

    def _download_model_from_hf(self, model_name='', model_dir=''):

        # refer to https://huggingface.co/docs/huggingface_hub/v0.23.1/en/package_reference/file_download
        if self.token == '':
            self.token = None
        elif self.token.lower() == 'true':
            self.token = True
        # else token would be a string from the user.
        env_vars = {'https_proxy': lazyllm.config['https_proxy'] or os.environ.get("https_proxy", None),
                    'http_proxy': lazyllm.config['http_proxy'] or os.environ.get("http_proxy", None)}
        with EnvVarContextManager(env_vars):
            if not os.environ.get("https_proxy", None):
                lazyllm.LOG.warning('If there is no download response or if downloads repeatedly fail over an '
                                    'extended period, please set the `LAZYLLM_HTTPS_PROXY` environment variable '
                                    'to configure a proxy. Do not directly set the `https_proxy` and `http_proxy` '
                                    'environment variables in your environment, as doing so may disrupt model '
                                    'deployment and result in deployment failures.')
            with self.progress:
                from huggingface_hub import snapshot_download
                model_dir_result = snapshot_download(repo_id=model_name, local_dir=model_dir, token=self.token)

        lazyllm.LOG.info(f"model downloaded at {model_dir_result}")
        return model_dir_result

    def _download_model_from_ms(self, model_name='', model_dir=''):

        # refer to https://www.modelscope.cn/docs/models/download
        if (len(self.token) > 0):
            from modelscope.hub.api import HubApi
            api = HubApi()
            api.login(self.token)

        with self.progress:
            from modelscope.hub.snapshot_download import snapshot_download
            model_dir_result = snapshot_download(model_id=model_name, local_dir=model_dir)

        lazyllm.LOG.info(f"Model downloaded at {model_dir_result}")
        return model_dir_result

def custom_tqdm_factory():
    instance = []  # Stores tqdm objects in all threads in the current context

    class CustomTqdm(tqdm.std.tqdm):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            if self not in instance:
                instance.append(self)

    return CustomTqdm, instance

class ProgressTracker:
    def __init__(self):
        self.tqdm_list = []  # Stores tqdm objects in all threads in the current context
        self.origin_tqdm = tqdm.std.tqdm
        self.start_flag = True
        self.polling_thread = None
        self.call_back = None

    def set_callback(self, callback):
        if not callable(callback):
            raise ValueError("Callback must be callable.")
        self.call_back = callback

    def polling_progress(self):
        while self.start_flag:
            n, total = self.get_progress()
            if callable(self.call_back):
                try:
                    self.call_back(n, total)
                except Exception as e:
                    print(f"Error in callback: {e}")
            time.sleep(1)

    def get_progress(self):
        n, total = 0, 0
        for tqdm_obj in self.tqdm_list:
            n += tqdm_obj.n
            total += tqdm_obj.total
        return n, total

    def __enter__(self):
        CustomTqdm, self.tqdm_list = custom_tqdm_factory()
        tqdm.std.tqdm = CustomTqdm
        if self.call_back:
            self.polling_thread = threading.Thread(target=self.polling_progress)
            self.polling_thread.daemon = True
            self.polling_thread.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        time.sleep(1)  # Wait for the check to stabilize and get the result when the model has been downloaded
        self.start_flag = False
        if self.call_back:
            self.polling_thread.join()
        tqdm.std.tqdm = self.origin_tqdm
        self.call_back = None
        self.tqdm_list = []
