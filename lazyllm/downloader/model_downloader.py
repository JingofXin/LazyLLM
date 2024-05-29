import os
import json
import shutil
import lazyllm
from .model_mapping import model_name_mapping

lazyllm.config.add('model_source', str, '', 'MODEL_SOURCE')
lazyllm.config.add('model_cache_dir', str, os.path.join(os.path.expanduser('~'), '.lazyllm', 'model'), 
                   'MODEL_CACHE_DIR')
lazyllm.config.add('model_source_token', str, '', 'MODEL_SOURCE_TOKEN')


class ModelDownloader():
    def __init__(self, model_source='', token=lazyllm.config['model_source_token'], 
                 cache_dir=lazyllm.config['model_cache_dir'], conf_file=''):
        self.model_source=model_source
        self.token=token
        self.cache_dir=cache_dir
        
    def download(self, model=''):
        assert isinstance(model, str), "model name should be a string."
        if len(model) == 0 or model[0] in (os.sep, '.', '~'): return model #Dummy or local model.
        if self.model_source == '':
            return model

        if model in model_name_mapping.keys() \
                    and self.model_source in model_name_mapping[model].keys():
            full_model_dir = os.path.join(self.cache_dir, model)
            if self._is_model_valid(full_model_dir):
                print(f"[INFO] model link found at {full_model_dir}")
                return full_model_dir
            else:
                self._unlink_or_remove_model(full_model_dir)
            
            mapped_model_name = model_name_mapping[model][self.model_source]
            model_save_dir = self._do_download(mapped_model_name)
            if model_save_dir:
                os.symlink(model_save_dir, full_model_dir, target_is_directory=True)
                return full_model_dir
            return model #failed to download model, keep model as it is
        else:
            model_save_dir = self._do_download(model)
            return model_save_dir if model_save_dir else model
 
    def _is_model_valid(self, model_dir):
        if not os.path.isdir(model_dir):
            return False
        return any((True for _ in os.scandir(model_dir)))
    
    def _unlink_or_remove_model(self, model_dir):
        if not os.path.exists(model_dir): return
        if os.path.islink(model_dir):
            os.unlink(model_dir)
        else:
            shutil.rmtree(model_dir)       
    
    def _do_download(self, model=''):
        model_dir = model.replace('/', os.sep)
        full_model_dir = os.path.join(self.cache_dir, model_dir)
        if self._is_model_valid(full_model_dir):
            print(f"[INFO] model found at {full_model_dir}")
            return full_model_dir
        else:
            self._unlink_or_remove_model(full_model_dir)
        
        if self.model_source == 'huggingface':
            return self._download_model_from_hf(model, full_model_dir)
        elif self.model_source == 'modelscope':
            return self._download_model_from_ms(model, self.cache_dir)
        
        print("[WARNING] model automatic downloads only support Huggingface and Modelscope currently.")
        return model
    
    def _download_model_from_hf(self, model_name = '', model_dir = ''):
        from huggingface_hub import snapshot_download
        
        try:
            # refer to https://huggingface.co/docs/huggingface_hub/v0.23.1/en/package_reference/file_download
            # #huggingface_hub.snapshot_download
            if self.token == '':
                self.token = None
            elif self.token.lower() == 'true':
                self.token = True
            #else token would be a string from the user.
            
            model_dir_result = snapshot_download(repo_id=model_name, local_dir=model_dir, token=self.token)  
            
            print(f"[INFO] model downloaded at {model_dir_result}")
            return model_dir_result
        except Exception as e:
            print(f"[ERROR] Huggingface: {e}")
            if os.path.isdir(model_dir):
                shutil.rmtree(model_dir)
                print(f"[ERROR] {model_dir} removed due to exceptions.") 
                #so that lazyllm would not regard model_dir as a downloaded available model after.        

    def _download_model_from_ms(self, model_name = '', model_source_dir = ''):        
        from modelscope.hub.snapshot_download import snapshot_download
        #refer to https://www.modelscope.cn/docs/ModelScope%20Hub%E4%BD%BF%E7%94%A8%E6%96%87%E6%A1%A3
        try:
            if (len(self.token) > 0):
                from modelscope.hub.api import HubApi
                api = HubApi()
                api.login(self.token)        
            model_dir_result = snapshot_download(model_id=model_name, cache_dir=model_source_dir)
                       
            print(f"[INFO] model downloaded at {model_dir_result}")
            return model_dir_result
        except Exception as e:
            print(f"[ERROR] Modelscope:{e}")
        
            # unlike Huggingface, Modelscope adds model name as sub-dir to cache_dir.
            # so we need to figure out the exact dir of the model for clearing in case of exceptions.     
            model_dir = model_name.replace('/', os.sep)
            full_model_dir = os.path.join(model_source_dir, model_dir)    
            if os.path.isdir(full_model_dir):
                shutil.rmtree(full_model_dir)
                print(f"[ERROR] {full_model_dir} removed due to exceptions.")        
