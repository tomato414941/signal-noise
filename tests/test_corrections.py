from signal_noise.evaluator.corrections import bonferroni, fdr_bh


class TestBonferroni:
    def test_basic(self):
        pvalues = [0.01, 0.04, 0.03, 0.20]
        result = bonferroni(pvalues, alpha=0.05)
        # threshold = 0.05/4 = 0.0125
        assert result == [True, False, False, False]

    def test_all_significant(self):
        pvalues = [0.001, 0.002, 0.003]
        result = bonferroni(pvalues, alpha=0.05)
        assert result == [True, True, True]

    def test_none_significant(self):
        pvalues = [0.5, 0.6, 0.7]
        result = bonferroni(pvalues, alpha=0.05)
        assert result == [False, False, False]

    def test_empty(self):
        assert bonferroni([]) == []

    def test_single_value(self):
        assert bonferroni([0.01], alpha=0.05) == [True]
        assert bonferroni([0.10], alpha=0.05) == [False]


class TestFDR:
    def test_basic(self):
        pvalues = [0.01, 0.04, 0.03, 0.20]
        result = fdr_bh(pvalues, alpha=0.05)
        # Sorted: 0.01(idx0), 0.03(idx2), 0.04(idx1), 0.20(idx3)
        # Thresholds: 1/4*0.05=0.0125, 2/4*0.05=0.025, 3/4*0.05=0.0375, 4/4*0.05=0.05
        # 0.01 <= 0.0125 -> True (k=1)
        # 0.03 <= 0.025 -> False
        # max_k=1, only index 0 significant
        assert result[0] is True
        assert result[3] is False

    def test_step_up_procedure(self):
        pvalues = [0.005, 0.011, 0.02, 0.04, 0.50]
        result = fdr_bh(pvalues, alpha=0.05)
        # Sorted: 0.005, 0.011, 0.02, 0.04, 0.50
        # Thresholds: 0.01, 0.02, 0.03, 0.04, 0.05
        # 0.005<=0.01 T, 0.011<=0.02 T, 0.02<=0.03 T, 0.04<=0.04 T, 0.50<=0.05 F
        # max_k=4
        assert result == [True, True, True, True, False]

    def test_all_significant(self):
        pvalues = [0.001, 0.002, 0.003]
        result = fdr_bh(pvalues, alpha=0.05)
        assert result == [True, True, True]

    def test_none_significant(self):
        pvalues = [0.5, 0.6, 0.7]
        result = fdr_bh(pvalues, alpha=0.05)
        assert result == [False, False, False]

    def test_empty(self):
        assert fdr_bh([]) == []

    def test_preserves_original_order(self):
        pvalues = [0.20, 0.005, 0.04, 0.01]
        result = fdr_bh(pvalues, alpha=0.05)
        assert result[1] is True
        assert result[3] is True
