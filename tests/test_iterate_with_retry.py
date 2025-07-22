def test_iterate_with_retry_recovers(monkeypatch, tweets_module):
    items = [{'id': 0}, {'id': 1}, {'id': 2}]

    class DummyDataset:
        def __init__(self):
            self.calls = 0
        def iterate_items(self, offset=0):
            self.calls += 1
            for idx in range(offset, len(items)):
                if self.calls == 1 and idx == 1:
                    raise RuntimeError('fail')
                yield items[idx]

    class DummyClient:
        def __init__(self, dataset):
            self._dataset = dataset
        def dataset(self, dataset_id):
            assert dataset_id == 'ds1'
            return self._dataset

    dummy = DummyDataset()
    client = DummyClient(dummy)
    monkeypatch.setattr(tweets_module, 'retry_func', lambda func, *a, **kw: func(*a, **kw))

    result = list(tweets_module.iterate_with_retry(client, 'ds1'))

    assert result == items
    assert dummy.calls >= 2
