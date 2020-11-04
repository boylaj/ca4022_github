#script name: genre_col_to_row.py
import sys
import os
import re
 

 
if __name__ == "__main__":
    # get set containing all genres and count of each movie that has that genre
    # e.g {'action': 4, 'comedy': 3, 'horror': 1}
    # And a set of genres and sum of avg ratings
    # e.g {'action': 401, 'comedy': 867436.3, 'horror': 546.76}
    # And count of ratings per genre

    allgenres = {}
    allgenrescores = {}
    ratingspergenre = {}    
    for line in sys.stdin:
        tokens = line.split("\t")
        ratingCount = tokens[2]
        avgRating = tokens[3]
        category = re.sub(r'[\(\)]', r'', tokens[-1])
        category = category.strip().split(",")

        for gen in category:
            if gen not in allgenres:
                allgenres[gen] = 1
                allgenrescores[gen] = float(avgRating)
                ratingspergenre[gen] = float(ratingCount)
            else:
                allgenres[gen] += 1
                allgenrescores[gen] += float(avgRating)
                ratingspergenre[gen] += float(ratingCount)

    for g in allgenres:
        print("\t".join([g,
        str(allgenres[g]),
        str(allgenrescores[g] / allgenres[g]),
        str(ratingspergenre[g])]))

