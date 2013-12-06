# File to convert a csv file containing nominal and numeric features to numeric features
# If a feature is not in quotes, that means it is numeric.

NULL_WORDS = ["NULL", "NA", "", "Null", "null", "na", "None", "none"]


def normalize_feature(feature_vector):
  n = sum([1 if f is not in NULL_WORDS else 0 for f in feature_vector])
  complete_feature_vector = [f  if f is not in NULL_WORDS else "0" for f in feature_vector]
  try:
    complete_feature_vector = map(float, complete_feature_vector)
  except Exception, e:
    print "Error in data format"
  mean = sum(complete_feature_vector)/n
  mean0_fvector = [(f-mean) for f in complete_feature_vector]
  sum_squares = sum([newf*newf for newf in mean0_fvector])
  variance1_fvector = [newf*n/sum_squares for newf in mean0_fvector]
  return variance1_fvector
  
def 


if __name__=="main":
  infile = open("~/datasets/manmachine/popcore_data/weka_share_data_cleancolumns.csv")
  data = []
  for line in infile:
    line = line.trim("\n")
    data.append(line.split(","))
  num_features = len(data[1])
  for j in range(num_features):
    single_feature_vector = [row[j] for row in data]
    is_numeric_feature = True
    # Aggressive conversion to string or nominal. As even one invalid number can 
    # mess up number conversion.
    for val in single_feature_vector:
      if val.startswith("'") or val.startswith("\""):
        is_numeric_feature = False
        break
    if is_numeric_feature:
      new_feature = normalize_feature(single_feature_vector)
      for i in range(len(data)):
        data[i][j] = new_feature[i]
    else:
      #create one feature each for each value of a nominal/string feature
         

