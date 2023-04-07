using Downloads, ZipFile, CSV, DataFrames

"""
    download_atus_data(data_file, year)

Download the given ATUS data file. This saves the zip file in a subdirectory called "data".
This also opens one of the files and returns it as a DataFrame object.

# Arguments
- `data_file`: The type of ATUS data you want to download. One of `("resp", "rost")`.
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


# resp2021 = download_atus_data("resp","2021")