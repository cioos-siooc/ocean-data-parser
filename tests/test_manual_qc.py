from ocean_data_parser.process import manual_qc


class TestFlagConvention:
    def test_get_qartod_flag_convention(self):
        attrs = manual_qc.get_manual_flag_attributes("QARTOD")
        assert attrs
        assert "flag_meaning" in attrs
        assert "flag_values" in attrs
        assert "long_name" in attrs
