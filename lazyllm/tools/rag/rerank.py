from functools import lru_cache
from typing import Callable, List, Optional, Union

import lazyllm
from lazyllm import ModuleBase, LOG
from lazyllm.tools.rag.store import DocNode, MetadataMode
from .retriever import _PostProcess


class Reranker(ModuleBase, _PostProcess):
    registered_reranker = dict()

    def __init__(self, name: str = "ModuleReranker", target: Optional[str] = None,
                 output_format: Optional[str] = None, join: Union[bool, str] = False, **kwargs) -> None:
        super().__init__()
        self._name = name
        self._kwargs = kwargs
        if self._name == "ModuleReranker":
            self._reranker = lazyllm.TrainableModule(self._kwargs['model'])
        _PostProcess.__init__(self, target, output_format, join)

    def forward(self, nodes: List[DocNode], query: str = "") -> List[DocNode]:
        if self._name == "ModuleReranker":
            docs = [node.get_text(metadata_mode=MetadataMode.EMBED) for node in nodes]
            top_n = self._kwargs['topk'] if 'topk' in self._kwargs else len(docs)
            if self._reranker._deploy_type == lazyllm.deploy.Infinity:
                sorted_indices = self._reranker(query, documents=docs, top_n=top_n)
            else:
                inps = {'query': query, 'documents': docs, 'top_n': top_n}
                sorted_indices = self._reranker(inps)
            results = [nodes[i] for i in sorted_indices]
        else:
            results = self.registered_reranker[self._name](nodes, query=query, **self._kwargs)
        LOG.debug(f"Rerank use `{self._name}` and get nodes: {results}")
        return self._post_process(results)

    @classmethod
    def register_reranker(
        cls: "Reranker", func: Optional[Callable] = None, batch: bool = False
    ):
        def decorator(f):
            def wrapper(nodes, **kwargs):
                if batch:
                    return f(nodes, **kwargs)
                else:
                    results = [f(node, **kwargs) for node in nodes]
                    return [result for result in results if result]

            cls.registered_reranker[f.__name__] = wrapper
            return wrapper

        return decorator(func) if func else decorator


@lru_cache(maxsize=None)
def get_nlp_and_matchers(language):
    import spacy
    from spacy.matcher import PhraseMatcher

    nlp = spacy.blank(language)
    required_matcher = PhraseMatcher(nlp.vocab)
    exclude_matcher = PhraseMatcher(nlp.vocab)
    return nlp, required_matcher, exclude_matcher


@Reranker.register_reranker
def KeywordFilter(
    node: DocNode,
    required_keys: List[str] = [],
    exclude_keys: List[str] = [],
    language: str = "en",
    **kwargs,
) -> Optional[DocNode]:
    assert required_keys or exclude_keys, 'One of required_keys or exclude_keys should be provided'
    nlp, required_matcher, exclude_matcher = get_nlp_and_matchers(language)
    if required_keys:
        required_matcher.add("RequiredKeywords", list(nlp.pipe(required_keys)))
    if exclude_keys:
        exclude_matcher.add("ExcludeKeywords", list(nlp.pipe(exclude_keys)))

    doc = nlp(node.get_text())
    if required_keys and not required_matcher(doc):
        return None
    if exclude_keys and exclude_matcher(doc):
        return None
    return node

# User-defined similarity decorator
def register_reranker(func=None, batch=False):
    return Reranker.register_reranker(func, batch)
