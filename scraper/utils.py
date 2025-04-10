############## SAVING DATA - not in use, left just in case

def save_data_to_excel(data:dict, file_name:str="output_data/data.xlsx"):
    """
    Saves the provided data to an Excel file. If the file exists, new data is 
    appended to the existing sheet.

    Parameters:
        data (dict): A dictionary containing the data to be saved to the Excel file.
        file_name (str): The path to the Excel file where the data will be saved 
        (default is 'output_data/data.xlsx').

    Raises:
        FileNotFoundError: If the Excel file does not exist and cannot be created.
    """
    df = pd.DataFrame([data])

    try:
        with pd.ExcelWriter(file_name, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
            book = writer.book
            sheet = book['oferty']
            
            start_row = sheet.max_row + 1  # pierwsza wolna linia

            for r_idx, row in df.iterrows():
                for c_idx, value in enumerate(row):
                    sheet.cell(row=start_row + r_idx, column=c_idx + 1, value=value)

    except FileNotFoundError:
        with pd.ExcelWriter(file_name, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, header=True, sheet_name='oferty')

    print(f"Data has been successfully saved to {file_name}")