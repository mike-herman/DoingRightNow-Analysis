using DataFrames, Arrow, CategoricalArrays, ScientificTypes, MLJ
import MLJBase

DATA_FILE_PATH = "./data/model_data.arrow";
df = DataFrame(Arrow.Table(DATA_FILE_PATH));
df = copy(df);


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

clean_data!(df);
y, X = unpack(df, ==(:TRTIER2));
train, test = partition(eachindex(y), 0.8)

# Load models from packages.
RandomForestClassifier = @load RandomForestClassifier pkg=DecisionTree

# Define a new model struct.
mutable struct ATUSRandomForest <: ProbabilisticNetworkComposite
    preprocessor    # This part does the pre-processing.
    classifier    # This part does the classifying
end

# Create prefit
function MLJBase.prefit(composite::ATUSRandomForest, verbosity, X, y)

    # Learning network
    Xs = source(X)
    ys = source(y)
    mach1 = machine(:preprocessor, Xs)
    x = MLJ.transform(mach1, Xs)
    mach2 = machine(:classifier, x, ys)
    yhat = predict(mach2, x)

    verbosity > 0 && @info "I sure am noisy"

    # return "learning network interface":
    return (; predict=yhat)

end


one_hot_encoder = OneHotEncoder()
forest = RandomForestClassifier(
    n_subfeatures=12,
    sampling_fraction=0.3,    # We have lots of data. Only use 30%.
    max_depth=10,
    rng=71
    )

atus_random_forest = ATUSRandomForest(one_hot_encoder,forest)

mach = machine(atus_random_forest, X, y)
fit!(mach)
ŷ = predict(mach, X[test,:])
cross_entropy(ŷ, y[test])

coerce(ŷ, Multiclass)





# Trying a logistic regression.
# https://docs.juliahub.com/MLJLinearModels/FBSRA/0.4.0/quickstart/
MultinomialClassifier = @load MultinomialClassifier pkg = MLJLinearModels

# Define new model struct
mutable struct ATUSMultinomialClassifier <: ProbabilisticNetworkComposite
    preprocessor    # This part does the pre-processing.
    classifier    # This part does the classifying
end

# Create prefit
function MLJBase.prefit(composite::ATUSMultinomialClassifier, verbosity, X, y)

    # Learning network
    Xs = source(X)
    ys = source(y)
    mach1 = machine(:preprocessor, Xs)
    x = MLJ.transform(mach1, Xs)
    mach2 = machine(:classifier, x, ys)
    yhat = predict(mach2, x)

    verbosity > 0 && @info "I sure am noisy"

    # return "learning network interface":
    return (; predict=yhat)

end

one_hot_encoder = OneHotEncoder()
multinomial_classifier = MultinomialClassifier(penalty=:l1)

atus_multinomial_classifier = ATUSMultinomialClassifier(one_hot_encoder,multinomial_classifier)
mach = machine(atus_multinomial_classifier, X, y)
fit!(mach)
fitted_params(mach)
ŷ = predict(mach, X[test,:])
cross_entropy(ŷ, y[test])

# Multivariate logistic regression doesn't really do anything here.