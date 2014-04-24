#!/usr/bin/python

# File to convert a csv file containing nominal and numeric features to numeric features
# If a feature has any one value that is not a null_word and is convertible to float, then it is a nominal/category feature.
import math
import argparse

NULL_WORDS = ["NULL", "NA", "", "Null", "null", "na", "None", "none"]
LABELS = [-1, 1]
NUMERIC_FEAT_PREFIX = "numeric&#"
HAS_HEADER = True
LABEL_COLUMN = "last"  # possible values are "first", "last"

#TODO add support for label at any column.
#TODO remove feature in case each one is NA or "NA"
def normalize_feature(feature_vector):
    n = sum([1 if f not in NULL_WORDS else 0 for f in feature_vector])
    complete_feature_vector = [f if f not in NULL_WORDS else "0" for f in feature_vector]
    try:
        complete_feature_vector = map(float, complete_feature_vector)
    except Exception, e:
        print "Error in data format"
        exit(1)
    mean = sum(complete_feature_vector) / n
    mean0_fvector = [(f - mean) for f in complete_feature_vector]
    sum_squares = sum([newf * newf for newf in mean0_fvector])
    if sum_squares == 0:
        variance1_fvector = [0 for newf in mean0_fvector]
    else:
        variance1_fvector = [newf * math.sqrt(n / sum_squares) for newf in mean0_fvector]
    return variance1_fvector


def is_numeric_feature(feature_vector):
    is_numeric_feature = True
    # Aggressive conversion to string or nominal. As even one invalid number can
    # mess up number conversion.
    for val in feature_vector:
        try:
            num_val = float(val)
        except ValueError, ve:
            if val not in NULL_WORDS:
                is_numeric_feature = False
                break
        """
        if val.startswith("'") or val.startswith("\""):
          is_numeric_feature = False
          break
        elif val not in NULL_WORDS:
          try:
            float
          is_numeric_feature = False
          break
        """
    return is_numeric_feature


def parse_label_column(label_vector):
    new_label_vector = [None] * len(label_vector)
    label_names = {}
    k = 0
    for j in range(len(label_vector)):
        if label_vector[j] not in label_names:
            try:
                label_names[label_vector[j]] = LABELS[k]
            except Exception, e:
                print "Invalid number of labels. Only 2 are supported"
                exit(1)
            k += 1
        new_label_vector[j] = label_names[label_vector[j]]
    return new_label_vector


def convert_to_libsvm(csv_file, libsvm_file, has_header=HAS_HEADER, label_column=LABEL_COLUMN):
    data = []
    global_features = {}
    findex = 1
    num_features = None
    line_number = 0
    for line in csv_file:
        line_number += 1
        line = line.strip("\n")
        if line == "" or (line_number == 1 and has_header):
            continue
        cols = line.split(",")
        if num_features is None:
            num_features = len(cols)
        if len(cols) != num_features:
            print "Invalid row at line %d. Expecting %d columns but got %d." % (line_number, num_features, len(cols))
            exit(1)
        if label_column == "last":
            feature_vals = [val.strip(" \n") for val in cols[:-1]]
            label_val = cols[-1]
        else:
            feature_vals = [val.strip(" \n") for val in cols[1:]]
            label_val = cols[0]
        row_vals = [label_val]
        row_vals.extend(feature_vals)
        data.append(row_vals)

    # Label has to be the first column
    label_vector = [row[0] for row in data]
    new_label_vector = parse_label_column(label_vector)
    for i in range(len(data)):
        data[i][0] = new_label_vector[i]

    #assuming the first row is a class row
    #TODO change textual labels/labels to integer
    for j in range(1, num_features):
        single_feature_vector = [row[j] for row in data]
        if is_numeric_feature(single_feature_vector):
            new_feature = normalize_feature(single_feature_vector)
            for i in range(len(data)):
                data[i][j] = new_feature[i]
            global_features[NUMERIC_FEAT_PREFIX + str(j)] = findex
            findex += 1
        else:  #create one feature each for each value of a nominal/string feature
            for i in range(len(data)):
                data[i][j] = data[i][j].strip("\n\"\'")
            single_feature_vector = [row[j] for row in data]
            for feat in single_feature_vector:
                if feat not in global_features:
                    global_features[feat] = findex
                    findex += 1
    #print data

    #creating and writing features to output libsvm format
    for i in range(len(data)):
        libsvm_line = str(data[i][0])
        for j in range(1, len(data[i])):
            if (NUMERIC_FEAT_PREFIX + str(j)) in global_features:
                if data[i][j] != 0:
                    libsvm_line += " " + str(global_features[NUMERIC_FEAT_PREFIX + str(j)]) + ":" + str(data[i][j])
            elif data[i][j] in global_features:
                libsvm_line += " " + str(global_features[data[i][j]]) + ":1"
        libsvm_file.write(libsvm_line + "\n")
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enter input and output files.")
    parser.add_argument('input_file')
    parser.add_argument('output_file')
    args = parser.parse_args()
    infile = open(args.input_file)
    outfile = open(args.output_file, "w")
    convert_to_libsvm(infile, outfile)
    infile.close()
    outfile.close()
    # a two-dimensional string array containing all the data from the csv file
  
    
        
              
      
         

