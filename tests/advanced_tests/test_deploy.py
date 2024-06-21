import json

import lazyllm
from lazyllm import deploy

class TestFn_Deploy(object):

    def setup_method(self):
        self.model_path = 'internlm2-chat-7b'
        self.inputs = ['介绍一下你自己', '李白和李清照是什么关系', '说个笑话吧']

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

    def test_embedding(self):
        m = lazyllm.TrainableModule('bge-large-zh-v1.5').deploy_method(deploy.AutoDeploy)
        m.update_server()
        res = m('你好')
        assert len(json.loads(res)) == 1024
