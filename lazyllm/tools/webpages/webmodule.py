import os
import json
import base64
import socket
import requests
import traceback
import multiprocessing
import gradio as gr
from PIL import Image
from io import BytesIO
from types import GeneratorType

import lazyllm
from lazyllm import LOG, globals
from lazyllm.flow import Pipeline
from ...module.module import ModuleBase


css = """
#logging {background-color: #FFCCCB}

#module {
  font-family: 'Courier New', Courier, monospace;
  font-size: 16px;
  white-space: pre !important;
}
"""

class WebModule(ModuleBase):
    class Mode:
        Dynamic = 0
        Refresh = 1
        Appendix = 2

    def __init__(self, m, *, components=dict(), title='对话演示终端', port=range(20500, 20799),
                 history=[], text_mode=None, trace_mode=None) -> None:
        super().__init__()
        self.m = lazyllm.ActionModule(m) if isinstance(m, lazyllm.FlowBase) else m
        self.title = title
        self.port = port
        components = sum([[([k._module_id, k._module_name] + list(v)) for v in vs]
                         for k, vs in components.items()], [])
        self.ckeys = [[c[0], c[2]] for c in components]
        self.history = [h._module_id for h in history]
        self.trace_mode = trace_mode if trace_mode else WebModule.Mode.Refresh
        self.text_mode = text_mode if text_mode else WebModule.Mode.Dynamic
        self._set_up_caching()
        self.demo = self.init_web(components)
        self.url = None

    def _set_up_caching(self):
        if 'GRADIO_TEMP_DIR' in os.environ:
            cach_path = os.environ['GRADIO_TEMP_DIR']
        else:
            cach_path = os.path.join(os.getcwd(), '.temp')
            os.environ['GRADIO_TEMP_DIR'] = cach_path
        if not os.path.exists(cach_path):
            os.makedirs(cach_path)

    def init_web(self, component_descs):
        with gr.Blocks(css=css, title=self.title) as demo:
            sess_data = gr.State(value={
                'sess_titles': [''],
                'sess_logs': {},
                'sess_history': {},
                'sess_num': 1,
                'curr_sess': '',
                'frozen_query': '',
            })
            with gr.Row():
                with gr.Column(scale=3):
                    with gr.Row():
                        gr.Textbox(elem_id='module', interactive=False, show_label=True,
                                   label="模型结构", value=repr(self.m))
                    with gr.Row():
                        chat_use_context = gr.Checkbox(interactive=True, value=False, label="使用上下文")
                    with gr.Row():
                        stream_output = gr.Checkbox(interactive=True, value=True, label="流式输出")
                        text_mode = gr.Checkbox(interactive=(self.text_mode == WebModule.Mode.Dynamic),
                                                value=(self.text_mode != WebModule.Mode.Refresh), label="追加输出")
                    components = []
                    for _, gname, name, ctype, value in component_descs:
                        if ctype in ('Checkbox', 'Text'):
                            components.append(getattr(gr, ctype)(interactive=True, value=value, label=f'{gname}.{name}'))
                        else:
                            raise KeyError(f'invalid component type: {ctype}')
                    with gr.Row():
                        dbg_msg = gr.Textbox(show_label=True, label='处理日志',
                                             elem_id='logging', interactive=False, max_lines=10)
                    clear_btn = gr.Button(value="🗑️  Clear history", interactive=True)
                with gr.Column(scale=6):
                    with gr.Row():
                        add_sess_btn = gr.Button("添加新会话")
                        sess_drpdn = gr.Dropdown(choices=sess_data.value['sess_titles'], label="选择会话：", value='')
                        del_sess_btn = gr.Button("删除当前会话")
                    chatbot = gr.Chatbot(height=900)
                    query_box = gr.Textbox(show_label=False, placeholder='输入内容并回车!!!')

            query_box.submit(self._init_session, [query_box, sess_data],
                                                 [sess_drpdn, chatbot, dbg_msg, sess_data], queue=True
                ).then(lambda: gr.update(interactive=False), None, query_box, queue=False
                ).then(lambda: gr.update(interactive=False), None, add_sess_btn, queue=False
                ).then(lambda: gr.update(interactive=False), None, sess_drpdn, queue=False
                ).then(lambda: gr.update(interactive=False), None, del_sess_btn, queue=False
                ).then(self._prepare, [query_box, chatbot, sess_data], [query_box, chatbot], queue=True
                ).then(self._respond_stream, [chat_use_context, chatbot, stream_output, text_mode] + components,
                                             [chatbot, dbg_msg], queue=chatbot
                ).then(lambda: gr.update(interactive=True), None, query_box, queue=False
                ).then(lambda: gr.update(interactive=True), None, add_sess_btn, queue=False
                ).then(lambda: gr.update(interactive=True), None, sess_drpdn, queue=False
                ).then(lambda: gr.update(interactive=True), None, del_sess_btn, queue=False)
            clear_btn.click(self._clear_history, [sess_data], outputs=[chatbot, query_box, dbg_msg, sess_data])

            sess_drpdn.change(self._change_session, [sess_drpdn, chatbot, dbg_msg, sess_data],
                                                    [sess_drpdn, chatbot, query_box, dbg_msg, sess_data])
            add_sess_btn.click(self._add_session, [chatbot, dbg_msg, sess_data],
                                                  [sess_drpdn, chatbot, query_box, dbg_msg, sess_data])
            del_sess_btn.click(self._delete_session, [sess_drpdn, sess_data],
                                                     [sess_drpdn, chatbot, query_box, dbg_msg, sess_data])
            return demo

    def _init_session(self, query, session):
        if session['curr_sess'] != '':  # remain unchanged.
            return gr.Dropdown(), gr.Chatbot(), gr.Textbox(), session
        session['frozen_query'] = query

        session['curr_sess'] = f"({session['sess_num']})  {query}"
        session['sess_num'] += 1
        session['sess_titles'][0] = session['curr_sess']

        session['sess_logs'][session['curr_sess']] = []
        session['sess_history'][session['curr_sess']] = []
        return gr.update(choices=session['sess_titles'], value=session['curr_sess']), [], '', session

    def _add_session(self, chat_history, log_history, session):
        if session['curr_sess'] == '':
            LOG.warning('Cannot create new session while current session is empty.')
            return gr.Dropdown(), gr.Chatbot(), gr.Textbox(), gr.Textbox(), session

        self._save_history(chat_history, log_history, session)

        session['curr_sess'] = ''
        session['sess_titles'].insert(0, session['curr_sess'])
        return gr.update(choices=session['sess_titles'], value=session['curr_sess']), [], '', '', session

    def _save_history(self, chat_history, log_history, session):
        if session['curr_sess'] in session['sess_titles']:
            session['sess_history'][session['curr_sess']] = chat_history
            session['sess_logs'][session['curr_sess']] = log_history

    def _change_session(self, session_title, chat_history, log_history, session):
        if session['curr_sess'] == '':  # new session
            return gr.Dropdown(), [], '', '', session

        if session_title not in session['sess_titles']:
            LOG.warning(f'{session_title} is not an existing session title.')
            return gr.Dropdown(), gr.Chatbot(), gr.Textbox(), gr.Textbox(), session

        self._save_history(chat_history, log_history, session)

        session['curr_sess'] = session_title
        return (gr.update(choices=session['sess_titles'], value=session['curr_sess']),
                session['sess_history'][session['curr_sess']], '',
                session['sess_logs'][session['curr_sess']], session)

    def _delete_session(self, session_title, session):
        if session_title not in session['sess_titles']:
            LOG.warning(f'session {session_title} does not exist.')
            return gr.Dropdown(), session
        session['sess_titles'].remove(session_title)

        if session_title != '':
            del session['sess_history'][session_title]
            del session['sess_logs'][session_title]
            session['curr_sess'] = session_title
        else:
            session['curr_sess'] = 'dummy session'
            # add_session and change_session cannot accept an uninitialized session.
            # Here we need to imitate removal of a real session so that
            # add_session and change_session could skip saving chat history.

        if len(session['sess_titles']) == 0:
            return self._add_session(None, None, session)
        else:
            return self._change_session(session['sess_titles'][0], None, None, session)

    def _prepare(self, query, chat_history, session):
        if not query:
            query = session['frozen_query']
        if chat_history is None:
            chat_history = []
        return '', chat_history + [[query, None]]

    def _respond_stream(self, use_context, chat_history, stream_output, append_text, *args):  # noqa C901
        try:
            # TODO: move context to trainable module
            input = chat_history[-1][0]
            history = chat_history[:-1] if use_context and len(chat_history) > 1 else None

            for k, v in zip(self.ckeys, args):
                if k[0] not in globals['global_parameters']: globals['global_parameters'][k[0]] = dict()
                globals['global_parameters'][k[0]][k[1]] = v

            if use_context:
                for h in self.history:
                    if h not in globals['chat_history']: globals['chat_history'] = dict()
                    globals['chat_history'][h] = history
            result = self.m(input)

            def get_log_and_message(s):
                if not self.trace_mode == WebModule.Mode.Appendix:
                    log_history.clear()
                if globals['err']: log_history.append(globals['err'][1])
                if globals['trace']: log_history.extend(globals['trace'])

                if isinstance(s, dict):
                    s = s.get("message", {}).get("content", "")
                else:
                    try:
                        r = json.loads(s)
                        if 'choices' in r:
                            if "type" not in r["choices"][0] or (
                                    "type" in r["choices"][0] and r["choices"][0]["type"] != "tool_calls"):
                                delta = r["choices"][0]["delta"]
                                if "content" in delta:
                                    s = delta["content"]
                                else:
                                    s = ""
                        elif 'images_base64' in r:
                            image_data = r.pop('images_base64')[0]
                            image = Image.open(BytesIO(base64.b64decode(image_data)))
                            return "The image is: ", "".join(log_history), image
                        else:
                            s = s
                    except (ValueError, KeyError, TypeError):
                        s = s
                    except Exception as e:
                        LOG.error(f"Uncaptured error `{e}` when parsing `{s}`, please contact us if you see this.")
                return s, "".join(log_history), None

            log_history = []
            if isinstance(result, (str, dict)):
                result, log, image = get_log_and_message(result)
            if image:
                chat_history[-1][1] = gr.Image(image)
            elif isinstance(result, str):
                chat_history[-1][1] = result
            elif isinstance(result, GeneratorType):
                # TODO(wzh/server): refactor this code
                chat_history[-1][1] = ''
                for s in result:
                    if isinstance(s, str):
                        s, log, _ = get_log_and_message(s)
                    chat_history[-1][1] = (chat_history[-1][1] + s) if append_text else s
                    if stream_output: yield chat_history, log
            elif isinstance(result, dict):
                chat_history[-1][1] = result.get("message", "")
            else:
                raise TypeError(f'function result should only be str, but got {type(result)}')
        except requests.RequestException as e:
            chat_history = None
            log = str(e)
        except Exception as e:
            chat_history = None
            log = f'{str(e)}\n--- traceback ---\n{traceback.format_exc()}'
            LOG.error(log)
        yield chat_history, log

    def _clear_history(self, session):
        session['sess_history'][session['curr_sess']] = []
        session['sess_logs'][session['curr_sess']] = []
        return [], '', '', session

    def _work(self):
        if isinstance(self.port, (range, tuple, list)):
            port = self._find_can_use_network_port()
        else:
            port = self.port
            assert self._verify_port_access(port), f'port {port} is occupied'

        def _impl():
            self.demo.queue().launch(server_name='0.0.0.0', server_port=port)
        self.p = multiprocessing.Process(target=_impl)
        self.p.start()
        self.url = f'http://0.0.0.0:{port}'

    def _get_deploy_tasks(self):
        return Pipeline(self._work)

    def _get_post_process_tasks(self):
        return Pipeline(self._print_url)

    def wait(self):
        if hasattr(self, 'p'):
            return self.p.join()

    def stop(self):
        if self.p and self.p.is_alive():
            self.p.terminate()
            self.p.join()

    def __repr__(self):
        return lazyllm.make_repr('Module', 'Web', name=self._module_name, subs=[repr(self.m)])

    def _find_can_use_network_port(self):
        for port in self.port:
            if self._verify_port_access(port):
                return port
        raise RuntimeError(
            f'The ports in the range {self.port} are all occupied. '
            'Please change the port range or release the relevant ports.'
        )

    def _verify_port_access(self, port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            result = s.connect_ex(('localhost', port))
            return result != 0

    def _print_url(self):
        LOG.success(f'LazyLLM webmodule launched successfully: Running on local URL: {self.url}', flush=True)
