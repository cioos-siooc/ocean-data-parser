import difflib


def compare_text_files(path1, path2):
    file1 = open(path1, "r")
    file2 = open(path2, "r")
    return list(
        difflib.unified_diff(
            file1.readlines(), file2.readlines(), fromfile=path1, tofile=path2, n=0
        )
    )
