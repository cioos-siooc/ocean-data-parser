import difflib


def compare_text_files(path1, path2):
    """Show differences between two text files."""
    with open(path1) as file1, open(path2) as file2:
        return list(
            difflib.unified_diff(
                file1.readlines(), file2.readlines(), fromfile=path1, tofile=path2, n=0
            )
        )
