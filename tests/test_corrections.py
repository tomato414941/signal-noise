from signal_noise.evaluator.corrections import bonferroni, fdr_bh


class TestBonferroni:
    def test_none_significant(self):
        pvals = [0.1, 0.2, 0.3, 0.4]
        result = bonferroni(pvals, alpha=0.05)
        assert result == [False, False, False, False]

    def test_one_significant(self):
        pvals = [0.001, 0.2, 0.3, 0.4]
        result = bonferroni(pvals, alpha=0.05)
        assert result == [True, False, False, False]

    def test_empty(self):
        assert bonferroni([], 0.05) == []


class TestFDR:
    def test_none_significant(self):
        pvals = [0.5, 0.6, 0.7, 0.8]
        result = fdr_bh(pvals, alpha=0.05)
        assert result == [False, False, False, False]

    def test_some_significant(self):
        pvals = [0.001, 0.01, 0.5, 0.9]
        result = fdr_bh(pvals, alpha=0.05)
        assert result[0] is True
        assert result[1] is True
        assert result[3] is False

    def test_empty(self):
        assert fdr_bh([], 0.05) == []

    def test_fdr_less_conservative_than_bonferroni(self):
        pvals = [0.005, 0.01, 0.02, 0.03]
        bon = bonferroni(pvals, 0.05)
        fdr = fdr_bh(pvals, 0.05)
        assert sum(fdr) >= sum(bon)
