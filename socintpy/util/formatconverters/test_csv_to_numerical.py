import csv_to_numerical as converter

if __name__ == "__main__":
  converter.convert_to_libsvm(open("test_convert.csv"), open("test_convert.libsvm", "w"), has_header=False, label_column="first")
    

