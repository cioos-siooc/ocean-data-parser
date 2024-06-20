def pytest_addoption(parser):
    parser.addoption(
        "--nerc-vocab",
        action="store_true",
        dest="nerc_vocab",
        default=False,
        help="enable nerc vocabulary tests",
    )
