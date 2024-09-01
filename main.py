from mt_tqdm import mt_map

some_list = list(range(100))
some_fn = lambda x: x+1

mt_map(some_fn, some_list, num_threads=2)