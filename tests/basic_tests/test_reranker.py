import unittest
from unittest.mock import patch, MagicMock
from lazyllm.tools.rag.store import DocNode
from lazyllm.tools.rag.rerank import RerankerV2, register_reranker


class TestRerankerV2(unittest.TestCase):

    def setUp(self):
        self.doc1 = DocNode(text="This is a test document with the keyword apple.")
        self.doc2 = DocNode(
            text="This is another test document with the keyword banana."
        )
        self.doc3 = DocNode(text="This document contains the keyword cherry.")
        self.nodes = [self.doc1, self.doc2, self.doc3]
        self.query = "test query"

    def test_keyword_filter_with_required_keys(self):
        required_keys = ["apple"]
        exclude_keys = []
        reranker = RerankerV2(
            name="KeywordFilter", required_keys=required_keys, exclude_keys=exclude_keys
        )
        results = reranker.forward(self.nodes, query=self.query)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].get_content(), self.doc1.get_content())

    def test_keyword_filter_with_exclude_keys(self):
        required_keys = []
        exclude_keys = ["banana"]
        reranker = RerankerV2(
            name="KeywordFilter", required_keys=required_keys, exclude_keys=exclude_keys
        )
        results = reranker.forward(self.nodes, query=self.query)
        self.assertEqual(len(results), 2)
        self.assertNotIn(self.doc2, results)

    @patch("lazyllm.components.utils.downloader.ModelManager.download")
    @patch("sentence_transformers.CrossEncoder")
    def test_module_reranker(self, MockCrossEncoder, mock_download):
        mock_model = MagicMock()
        mock_download.return_value = "mock_model_path"
        MockCrossEncoder.return_value = mock_model
        mock_model.predict.return_value = [0.8, 0.6, 0.9]

        reranker = RerankerV2(name="ModuleReranker", model="dummy-model", topk=2)
        results = reranker.forward(self.nodes, query=self.query)

        self.assertEqual(len(results), 2)
        self.assertEqual(
            results[0].get_content(), self.doc3.get_content()
        )  # highest score
        self.assertEqual(
            results[1].get_content(), self.doc1.get_content()
        )  # second highest score

    def test_register_reranker_decorator(self):
        @register_reranker
        def CustomReranker(node, **kwargs):
            if "custom" in node.get_content():
                return node
            return None

        custom_doc = DocNode(text="This document contains custom keyword.")
        nodes = [self.doc1, self.doc2, self.doc3, custom_doc]

        reranker = RerankerV2(name="CustomReranker")
        results = reranker.forward(nodes)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].get_content(), custom_doc.get_content())


if __name__ == "__main__":
    unittest.main()
