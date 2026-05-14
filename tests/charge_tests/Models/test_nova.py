import os

import pytest

import lazyllm
from lazyllm.components.formatter import decode_query_with_filepaths
from lazyllm.module.llms.onlinemodule.supplier.nova import NovaChat

from ...utils import get_api_key, get_path


CHAT_BASE_PATH = 'lazyllm/module/llms/onlinemodule/base/onlineChatModuleBase.py'
MULTIMODAL_BASE_PATH = 'lazyllm/module/llms/onlinemodule/base/onlineMultiModalBase.py'


def nova_api_key():
    api_key = get_api_key('nova')
    if not api_key:
        pytest.skip('LAZYLLM_NOVA_API_KEY is required')
    return api_key


class TestNova:
    @pytest.mark.ignore_cache_on_change(CHAT_BASE_PATH, get_path('nova'))
    def test_nova_chat(self):
        chat = lazyllm.OnlineChatModule(
            source='nova', model='sensenova-6.7-flash-lite', api_key=nova_api_key(), stream=False
        )
        result = chat('用一句中文介绍你自己。', max_tokens=64, reasoning_effort='none')
        assert isinstance(result, str)
        assert result

    @pytest.mark.ignore_cache_on_change(CHAT_BASE_PATH, get_path('nova'))
    def test_nova_validate_api_key(self):
        assert NovaChat(api_key=nova_api_key())._validate_api_key() is True
        assert NovaChat(api_key='invalid_api_key_12345')._validate_api_key() is False

    @pytest.mark.ignore_cache_on_change(MULTIMODAL_BASE_PATH, get_path('nova'))
    def test_nova_u1_fast_text2image(self):
        t2i = lazyllm.OnlineMultiModalModule(
            source='nova', type='text2image', model='sensenova-u1-fast', api_key=nova_api_key()
        )
        result = t2i('生成一张中文信息图，主题是 LazyLLM 的三项优势。', size='2048x2048')

        decoded = decode_query_with_filepaths(result)
        assert decoded['files']
        assert os.path.exists(decoded['files'][0])
