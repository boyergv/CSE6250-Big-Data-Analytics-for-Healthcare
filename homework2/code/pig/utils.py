@outputSchema("feature:chararray")
def bag_to_svmlight(input):
    return ' '.join(( "%s:%f" % (fid, float(fvalue)) for _, fid, fvalue in input))
