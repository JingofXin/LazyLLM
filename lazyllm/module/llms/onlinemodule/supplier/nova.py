from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests

import lazyllm
from lazyllm import config
from lazyllm.components.formatter import encode_query_with_filepaths
from lazyllm.components.utils.downloader.model_downloader import LLMType
from lazyllm.components.utils.file_operate import bytes_to_file
from ..base import OnlineChatModuleBase, LazyLLMOnlineText2ImageModuleBase


config.add('nova_model_name', str, 'sensenova-6.7-flash-lite', 'NOVA_MODEL_NAME',
           description='Default model name for Nova supplier.')
config.add('nova_text2image_model_name', str, 'sensenova-u1-fast', 'NOVA_TEXT2IMAGE_MODEL_NAME',
           description='Default model name for Nova text-to-image supplier.')


class NovaChat(OnlineChatModuleBase):
    MODEL_NAME = 'sensenova-6.7-flash-lite'
    VLM_MODEL_PREFIX = ['sensenova-6.7-flash-lite']

    def __init__(self, base_url: Optional[str] = None, model: Optional[str] = None,
                 api_key: str = None, stream: bool = True, return_trace: bool = False, **kwargs):
        base_url = base_url or 'https://token.sensenova.cn/v1/'
        model = model or lazyllm.config['nova_model_name'] or NovaChat.MODEL_NAME
        super().__init__(api_key=api_key or self._default_api_key(), base_url=base_url,
                         model_name=model, stream=stream, return_trace=return_trace, **kwargs)

    def _get_system_prompt(self):
        return 'You are an AI assistant developed by SenseTime.'

    def _convert_msg_format(self, msg: Dict[str, Any]):
        for choice in msg.get('choices', []):
            message = choice.get('message') or choice.get('delta') or {}
            if 'reasoning' in message and 'reasoning_content' not in message:
                message['reasoning_content'] = message['reasoning']
            if 'reasoning_content' in message and 'content' not in message:
                message['content'] = ''
        return msg

    def _validate_api_key(self):
        try:
            response = requests.get(urljoin(self._base_url, 'models'), headers=self._header, timeout=10)
            return response.status_code == 200
        except Exception:
            return False


class NovaText2Image(LazyLLMOnlineText2ImageModuleBase):
    MODEL_NAME = 'sensenova-u1-fast'

    def __init__(self, api_key: str = None, model: Optional[str] = None,
                 url: Optional[str] = None, return_trace: bool = False, **kwargs):
        url = url or kwargs.pop('base_url', None) or 'https://token.sensenova.cn/v1/'
        model = model or lazyllm.config['nova_text2image_model_name'] or NovaText2Image.MODEL_NAME
        super().__init__(api_key=api_key or self._default_api_key(), model=model,
                         url=url, return_trace=return_trace, **kwargs)
        if self._type == LLMType.IMAGE_EDITING:
            raise ValueError('Nova U1 Fast does not support image editing.')

    def _make_request(self, endpoint: str, payload: Dict[str, Any], base_url: Optional[str] = None,
                      timeout: int = 180) -> Dict[str, Any]:
        response = requests.post(urljoin(base_url or self._base_url, endpoint),
                                 headers=self._header, json=payload, timeout=timeout)
        response.raise_for_status()
        return response.json()

    def _download_image(self, image_url: str) -> bytes:
        self._validate_url_security(image_url)
        response = requests.get(image_url, timeout=180, allow_redirects=False)
        response.raise_for_status()
        self._validate_image_data(response.content, image_url)
        return response.content

    def _forward(self, input: str = None, files: List[str] = None, n: int = 1,
                 size: str = '2752x1536', model: str = None, url: str = None, **kwargs):
        if files:
            raise ValueError('Nova U1 Fast does not support reference images.')
        payload = {'model': model, 'prompt': input, 'size': size, 'n': n, **kwargs}
        result = self._make_request('images/generations', payload, base_url=url)
        image_urls = [item.get('url') for item in result.get('data', []) if item.get('url')]
        if not image_urls:
            raise Exception('No images returned from Nova API')
        image_bytes = [self._download_image(image_url) for image_url in image_urls]
        return encode_query_with_filepaths(None, bytes_to_file(image_bytes))
