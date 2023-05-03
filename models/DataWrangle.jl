module DataWrangle
export pull_and_clean_data

using DataFrames, Arrow, CategoricalArrays, ScientificTypes
using MLJBase: partition, unpack

function clean_data!(df)
    
    # Fix machine types.
    HEFAMINC_ordered_set = [
        "Less than 5,000",
        "5,000 to 7,499",
        "7,500 to 9,999",
        "10,000 to 12,499",
        "12,500 to 14,999",
        "15,000 to 19,999",
        "20,000 to 24,999",
        "25,000 to 29,999",
        "30,000 to 34,999",
        "35,000 to 39,999",
        "40,000 to 49,999",
        "50,000 to 59,999",
        "60,000 to 74,999",
        "75,000 to 99,999",
        "100,000 to 149,999",
        "150,000 and over"
    ]

    df.TRTIER2 = categorical(df.TRTIER2)
    df.GESTFIPS_label = categorical(df.GESTFIPS_label)
    df.HEFAMINC_label = categorical(df.HEFAMINC_label; levels=HEFAMINC_ordered_set, ordered=true)
    df.PEMARITL_label = categorical(df.PEMARITL_label)
    df.HETENURE_label = categorical(df.HETENURE_label)
    df.TUDIARYDAY_label = categorical(df.TUDIARYDAY_label)

    # drop columns and disallow missing.
    drop_cols = [
        :TUCASEID,:TUACTIVITY_N,:TUSTARTTIM,:TUSTOPTIME,
        :start_time_int,:stop_time_int,:TULINENO, :TUDIARYDAY
        ]
    select!(df, Not(drop_cols))
    disallowmissing!(df)

    # Define scientific types.
    coerce!(df, :snap_time_int => Continuous, :PRTAGE => Continuous)
end

"""
    pull_and_clean_data(DATA_FILE_PATH = "./data/model_data.arrow")

Pulls the arrow file stored in the given string.

Returns y, X, y_test, X_test
"""
function pull_and_clean_data(;DATA_FILE_PATH = "./data/model_data.arrow")
    
    df = DataFrame(Arrow.Table(DATA_FILE_PATH))
    df = copy(df)
    clean_data!(df)
    
    y, X = unpack(df, ==(:TRTIER2))
    
    train, test = partition(eachindex(y), 0.8)

    X_test = X[test, :]
    X = X[train,:]
    y_test = y[test]
    y = y[train];
    
    return y, X, y_test, X_test
end

end