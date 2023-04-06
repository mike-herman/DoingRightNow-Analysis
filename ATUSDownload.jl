#using Downloads, ZipFile, CSV, DataFrames

# Make the data a home.
function make_data_dir_if_not_exists()
    # Check if the the directory "data" exists in the working directory.
    # If not, create it.
    # Returns the filepath of the directory for future use.
    if !ispath("data")
        mkdir(joinpath("data"))
    end

    return joinpath("data")
end

DATA_DIR = make_data_dir_if_not_exists()

# Download ATUS 2021 Respondent file

RESPONDENT_FILE_URL = "https://www.bls.gov/tus/datafiles/atusresp-2021.zip"

resp = HTTP.request("GET", RESPONDENT_FILE_URL)
resp.body()
InfoZIP(resp, "./data/")


r = HTTP.get(RESPONDENT_FILE_URL, verbose = 0)
open_zip(r.body)


function get_respondent_file()
    r = HTTP.get(RESPONDENT)
    open_zip(r.body)
end


using Downloads, InfoZIP, CSV, DataFrames
make_data_dir_if_not_exists()
Downloads.download(RESPONDENT_FILE_URL,"./data/atusresp-2021.zip")


"""
    download_atus_data(data_file, year)

Download the given ATUS data file. This saves the zip file in a subdirectory called "data".
This also opens one of the files and returns it as a DataFrame object.

# Arguments
- `data_file`: The type of ATUS data you want to download. One of `("resp")`.
- `year`: The year you want to pull data for. Note: There is no data for 2020.

# Examples
```julia-repl
julia> bar([1, 2], [1, 2])
1
```
"""
function download_atus_data(data_file, year)
    year = string(year)
    zip_file_name = "atus$(data_file)-$(year).zip"
    file_url = "https://www.bls.gov/tus/datafiles/$(zip_file_name)"
    
    # Download the file to the data folder.
    if !ispath("data")
        mkdir(joinpath("data"))
    end

    f = Downloads.download(file_url, joinpath("data",zip_file_name))
    
    # Read the .dat (comma-seperated data) file into a DataFrame.
    data_file_name = "atus$(data_file)_$(year).dat"
    z = ZipFile.Reader(f)
    file_in_zip = filter(x->x.name == data_file_name, z.files)[1]
    df = DataFrame(CSV.File(file_in_zip))
    close(z)
        
    return df
end


resp2021 = download_atus_data("resp","2021")