# noqa: E121
import lazyllm
from typing import Any, Optional, List, Callable, Dict
from dataclasses import dataclass
from functools import partial

from lazyllm.tools.http_request.http_request import HttpRequest


@dataclass
class Node():
    id: int
    kind: str
    name: str
    args: Optional[Dict] = None
    func: Optional[Callable] = None
    arg_names: Optional[List[str]] = None


@dataclass
class NodeArgs(object):
    type: type
    default: Any = None
    options: Optional[List] = None
    getattr_f: Optional[Callable] = None


all_nodes = dict()

all_nodes['LocalLLM'] = dict(
    module=lazyllm.TrainableModule,
    init_arguments=dict(
        base_model=NodeArgs(str),
        target_path=NodeArgs(str),
        stream=NodeArgs(bool, True),
        return_trace=NodeArgs(bool, False)),
    builder_argument=dict(
        trainset=NodeArgs(str),
        prompt=NodeArgs(str),
        deploy_method=NodeArgs(str, 'vllm', getattr_f=partial(getattr, lazyllm.deploy)))
)

all_nodes['OnlineLLM'] = dict(
    module=lazyllm.OnlineChatModule,
    init_arguments=dict(
        source=NodeArgs(str),
        base_model=NodeArgs(str),
        base_url=NodeArgs(str),
        api_key=NodeArgs(str, None),
        secret_key=NodeArgs(str, None),
        stream=NodeArgs(bool, True),
        return_trace=NodeArgs(bool, False)),
    builder_argument=dict(
        prompt=NodeArgs(str)),
)

all_nodes['LocalEmbedding'] = dict(
    module=lazyllm.TrainableModule,
    init_arguments=dict(base_model=NodeArgs(str)),
    builder_argument=dict(deploy_method=NodeArgs(str, 'infinity', getattr_f=partial(getattr, lazyllm.deploy)))
)

all_nodes['OnlineEmbedding'] = dict(
    module=lazyllm.OnlineEmbeddingModule,
    init_arguments=dict(
        source=NodeArgs(str),
        embed_model_name=NodeArgs(str),
        embed_url=NodeArgs(str),
        api_key=NodeArgs(str, None),
        secret_key=NodeArgs(str, None))
)

all_nodes['SD'] = all_nodes['TTS'] = all_nodes['STT'] = dict(
    module=lazyllm.TrainableModule,
    init_arguments=dict(base_model=NodeArgs(str))
)

all_nodes['HTTP'] = dict(
    module=HttpRequest,
    init_arguments=dict(
        method=NodeArgs(str),
        url=NodeArgs(str),
        api_key=NodeArgs(str, ''),
        headers=NodeArgs(dict, {}),
        params=NodeArgs(dict, {}),
        body=NodeArgs(str, ''),
    ),
    builder_argument=dict(),
    other_arguments=dict()
)

all_nodes['Retriever'] = dict(
    module=lazyllm.tools.rag.Retriever,
    init_arguments=dict(
        doc=NodeArgs(Node),
        group_name=NodeArgs(str),
        similarity=NodeArgs(str, "cosine"),
        similarity_cut_off=NodeArgs(float, float("-inf")),
        index=NodeArgs(str, "default"),
        topk=NodeArgs(int, 6),
        target=NodeArgs(str, None),
        output_format=NodeArgs(str, None),
        join=NodeArgs(bool, False)
    )
)

all_nodes['Reranker'] = dict(
    module=lazyllm.tools.rag.Reranker,
    init_arguments=dict(
        name=NodeArgs(str, 'ModuleReranker'),
        target=NodeArgs(str, None),
        output_format=NodeArgs(str, None),
        join=NodeArgs(bool, False),
        arguments={
            '__name__': 'name',
            '__cls__': 'init_arguments',
            'ModuleReranker': dict(
                model=NodeArgs(str, 'bge-reranker-large'),
                topk=NodeArgs(int, -1)
            ),
            'KeywordFilter': dict(
                required_keys=NodeArgs(list, []),
                exclude_keys=NodeArgs(list, []),
                language=NodeArgs(str, "en")
            )
        }
    )
)


all_nodes["SqlManager"] = dict(
    module=lazyllm.tools.SqlManager,
    init_arguments=dict(
        db_type=NodeArgs(str, None),
        user=NodeArgs(str, None),
        password=NodeArgs(str, None),
        host=NodeArgs(str, None),
        port=NodeArgs(str, None),
        db_name=NodeArgs(str, None),
        options_str=NodeArgs(str, ""),
        tables_info_dict=NodeArgs(list, None),
    ),
)

all_nodes["SqlCall"] = dict(
    module=lazyllm.tools.SqlCall,
    init_arguments=dict(
        sql_manager=NodeArgs(Node),
        llm=NodeArgs(Node),
        sql_examples=NodeArgs(str, ""),
        use_llm_for_sql_result=NodeArgs(bool, True),
        return_trace=NodeArgs(bool, True),
    ),
)
