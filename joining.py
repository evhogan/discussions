import pandas as pd
from fuzzywuzzy import fuzz as fz
import operator

def return_highest(name_to_compare, list_names):
    pairs = list()
    for name in list_names:
        score = fz.ratio(name_to_compare, name)
        pairs.append((name, score))
    pairs.sort(key=operator.itemgetter(1), reverse = True)
    return(pairs[0])
    
def highest_pairs(main_frame, sub_frame, joiner):
    highest_names = []
    main_list = main_frame[joiner].to_list()
    sub_list = sub_frame[joiner].to_list()
    for main_name in main_list:
        highest_names.append(return_highest(main_name, sub_list))
    return(highest_names)
    
def join_metric(main_frame, sub_frame, joiner):
    list_frames = []
    pairs = highest_pairs(main_frame, sub_frame, joiner)
    names = [x[0] for x in pairs]
    scores = [x[1] for x in pairs]
    main_frame[f"closest_{joiner}"] = names
    main_frame[f"closest_score_{joiner}"] = scores
    list_series = []
    for main_row in main_frame.iterrows():
        for sub_row in sub_frame.iterrows():
            if main_row[1][f"closest_{joiner}"] == sub_row[1][joiner]:
                new_series = pd.concat([main_row[1], sub_row[1]], axis = 0)
                list_series.append(new_series)
    added = pd.concat(list_series, axis=1).T
    added = added.drop(joiner, axis=1)
    added.insert(loc = 0, column = f"true_{joiner}", value = main_frame[joiner].to_list())
    added.reset_index(drop = True, inplace = True)
    return(added)

def join_by_multiple(main_frame, sub_frame, joiners, tolerance = 35):
    main_dictrows = [x[1].to_dict() for x in main_frame.iterrows()]
    sub_dictrows = [x[1].to_dict() for x in sub_frame.iterrows()]
    list_rows = []
    counter = 0
    for main_row in main_dictrows:
        possible_rows = []
        for sub_row in sub_dictrows:
            score_dict = {}
            total_score = 0
            for joiner in joiners:
                score = fz.ratio(main_row[joiner], sub_row[joiner])
                score_dict[f"matched_{joiner}"] = sub_row[joiner]
                score_dict[f"matched_{joiner}_score"] = score
                total_score += score
            score_dict["average_score"] = total_score / len(joiners)
            if score_dict["average_score"] < tolerance:
                continue
            dict_all = {}
            for d in [score_dict, sub_row]:
                dict_all.update(d)
            possible_rows.append(dict_all)
            true_row = (sorted(possible_rows, key=lambda k: k['average_score'], reverse = True))[0]
        list_rows.append(true_row)
        counter += 1
        print(counter)
    scored_df = pd.DataFrame(list_rows)
    return(pd.concat([main_frame.reset_index(drop = True), scored_df.reset_index(drop = True)], axis= 1))


def join_by_multiple_SWS(main_frame, sub_frame, joiners, tolerance = 35):
    main_frame = pd.read_csv(main_frame).drop(['Unnamed: 0'],axis=1)
    sub_frame = pd.read_csv(sub_frame).drop(['Unnamed: 0'],axis=1)
    main_dictrows = [x[1].to_dict() for x in main_frame.iterrows()]
    sub_dictrows = [x[1].to_dict() for x in sub_frame.iterrows()]
    list_rows = []
    counter = 0
    for main_row in main_dictrows:
        possible_rows = []
        for sub_row in sub_dictrows:
            score_dict = {}
            total_score = 0
            for joiner in joiners:
                score = fz.ratio(str(main_row[joiner]), str(sub_row[joiner]))
                score_dict[f"matched_{joiner}"] = sub_row[joiner]
                score_dict[f"matched_{joiner}_score"] = score
                total_score += score
            score_dict["average_score"] = total_score / len(joiners)
            if score_dict["average_score"] < tolerance:
                continue
            dict_all = {}
            for d in [score_dict, sub_row]:
                dict_all.update(d)
            possible_rows.append(dict_all)
            true_row = (sorted(possible_rows, key=lambda k: k['average_score'], reverse = True))[0]
        list_rows.append(true_row)
        counter += 1
        print(counter)
    scored_df = pd.DataFrame(list_rows)
    return(pd.concat([main_frame.reset_index(drop = True), scored_df.reset_index(drop = True)], axis= 1).sort_values(by = "average_score", ascending = 0))