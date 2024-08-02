import os
import json
import time
import pytest
import httpx
import random
from gradio_client import Client

import lazyllm
from lazyllm import deploy
from lazyllm.launcher import cleanup

class TestDeploy(object):

    def setup_method(self):
        self.model_path = 'internlm2-chat-7b'
        self.inputs = ['介绍一下你自己', '李白和李清照是什么关系', '说个笑话吧']
        self.use_context = False
        self.stream_output = False
        self.append_text = False
        self.webs = []
        self.clients = []

    @pytest.fixture(autouse=True)
    def run_around_tests(self):
        yield
        while self.clients:
            client = self.clients.pop()
            client.close()
        while self.webs:
            web = self.webs.pop()
            web.stop()
        cleanup()

    def warp_into_web(self, module):
        client = None
        for _ in range(5):
            try:
                port = random.randint(10000, 30000)
                web = lazyllm.WebModule(module, port=port)
                web._work()
                time.sleep(2)
            except AssertionError as e:
                # Port is occupied
                if 'occupied' in e:
                    continue
                else:
                    raise e
            try:
                client = Client(web.url, download_files=web.cach_path)
                break
            except httpx.ConnectError:
                continue
        assert client, "Unable to create client"
        self.webs.append(web)
        self.clients.append(client)
        return web, client

    def test_deploy_lightllm(self):
        m = lazyllm.TrainableModule(self.model_path, '').deploy_method(deploy.lightllm)
        m.evalset(self.inputs)
        m.update_server()
        m.eval()
        assert len(m.eval_result) == len(self.inputs)

    def test_deploy_vllm(self):
        m = lazyllm.TrainableModule(self.model_path, '').deploy_method(deploy.vllm)
        m.evalset(self.inputs)
        m.update_server()
        m.eval()
        assert len(m.eval_result) == len(self.inputs)

    def test_deploy_auto(self):
        m = lazyllm.TrainableModule(self.model_path, '').deploy_method(deploy.AutoDeploy)
        m.evalset(self.inputs)
        m.update_server()
        m.eval()
        assert len(m.eval_result) == len(self.inputs)

    def test_deploy_auto_without_calling_method(self):
        m = lazyllm.TrainableModule(self.model_path, '')
        m.evalset(self.inputs)
        m.update_server()
        m.eval()
        assert len(m.eval_result) == len(self.inputs)

    def test_embedding(self):
        m = lazyllm.TrainableModule('bge-large-zh-v1.5').deploy_method(deploy.AutoDeploy)
        m.update_server()
        res = m('你好')
        assert len(json.loads(res)) == 1024

    def test_sd3(self):
        m = lazyllm.TrainableModule('stable-diffusion-3-medium')
        m.update_server()
        res = m('a little cat')
        assert "images_base64" in json.loads(res)

    def test_bark(self):
        m = lazyllm.TrainableModule('bark')
        m.update_server()
        res = m('你好啊，很高兴认识你。')
        assert "sounds" in json.loads(res)

    def test_stt_sensevoice(self):
        chat = lazyllm.TrainableModule('SenseVoiceSmall')
        m = lazyllm.ServerModule(chat)
        m.update_server()
        audio_path = os.path.join(lazyllm.config['data_path'], 'ci_data/shuidiaogetou.mp3')
        res = m(audio_path)
        assert '但愿人长久' in res

        _, client = self.warp_into_web(m)
        chat_history = [[audio_path, None]]
        ans = client.predict(self.use_context,
                             chat_history,
                             self.stream_output,
                             self.append_text,
                             api_name="/_respond_stream")
        res = ans[0][-1][-1]
        assert type(res) is str
        assert '但愿人长久' in res

    def test_vlm_and_lmdeploy(self):
        chat = lazyllm.TrainableModule('internvl-chat-2b-v1-5').deploy_method(deploy.LMDeploy)
        m = lazyllm.ServerModule(chat)
        m.update_server()
        query = '这是啥？'
        image_path = os.path.join(lazyllm.config['data_path'], 'ci_data/ji.jpg')
        res = m('lazyllm_files::' + json.dumps({'text': query, 'files': image_path}))
        assert '鸡' in res

        _, client = self.warp_into_web(m)
        # Add prefix 'lazyllm_img::' for client testing.
        chat_history = [['lazyllm_img::' + image_path, None], [query, None]]
        ans = client.predict(self.use_context,
                             chat_history,
                             self.stream_output,
                             self.append_text,
                             api_name="/_respond_stream")
        res = ans[0][-1][-1]
        assert '鸡' in res
