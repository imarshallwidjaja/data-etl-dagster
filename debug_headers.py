from libs.spatial_utils import normalize_tabular_headers
headers = ["a" * 65 + "x", "a" * 65 + "y", "a" * 65 + "z"]
result = normalize_tabular_headers(headers)
print('Result:', result)
print('Values:', list(result.values()))
