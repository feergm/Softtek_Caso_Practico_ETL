import os
import pandas as pd
import time 

current_time = time.strftime("%d-%m-%Y %H_%M_%S")
only_date = time.strftime("%d-%m-%Y")

def main():
    # Cargar datos desde archivos CSV
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Construye la ruta completa al archivo CSV
    csv_file_path_Employees = os.path.join(current_dir, "../assets/Employees.csv")
    csv_file_path_Salaries = os.path.join(current_dir, "../assets/Salaries.csv")

    # Lee el archivo CSV
    df_employees = pd.read_csv(csv_file_path_Employees)
    df_salaries = pd.read_csv(csv_file_path_Salaries)

    # Eliminar duplicados en los DataFrames originales si es necesario
    df_employees = df_employees.drop_duplicates(subset="EMPLOYEE_ID", keep="first")
    df_salaries = df_salaries.drop_duplicates(subset="EMPLOYEE_ID", keep="first")

    # Realizar una fusión (merge) en función de la columna "EMPLOYEE_ID", manteniendo todos los registros de "employees.csv"
    merged_df = pd.merge(df_employees, df_salaries, on="EMPLOYEE_ID", how="left")

    # Renombrar columnas según el layout deseado
    merged_df = merged_df.rename(columns={
        "EMPLOYEE_ID": "Employee_Key",
        "FIRST_NAME": "Name_First",
        "LAST_NAME": "Name_Last",
        "PHONE NUMBER": "Phone_Number",
        "FAX NUMBER": "Fax_Number",
        "EMAIL": "Email",
        "ADDRESS": "Address",
        "SALARY": "Salary",
        "DEALERSHIP_ID": "Dealership_Manager",
        "POSITION TYPE": "Position_Type",
        "CITY": "City",
        "STATE": "State",
        "REGION": "Region",
        "COUNTRY": "Country",
        "DATE_ENTERED": "Date_Entered"
    })

    # Concatenar los campos "Name_First" y "Name_Last" en la columna "Name"
    merged_df["Name"] = merged_df["Name_First"] + " " + merged_df["Name_Last"]

    merged_df["Phone_Number"] = merged_df["Phone_Number"].apply(format_phone)
    merged_df["Fax_Number"] = merged_df["Fax_Number"].apply(format_phone)

    # Crear un nuevo DataFrame para los registros de rechazo
    rejected_df = merged_df[(merged_df["Employee_Key"].isnull() | merged_df["Employee_Key"].eq("")) |
                            (merged_df["Dealership_Manager"].isnull() | merged_df["Dealership_Manager"].eq("")) |
                            (merged_df["Salary"].isnull())].copy()  # Copia el DataFrame para evitar la advertencia

    # Asignar 0 en el campo "Salary" cuando sea nulo en el DataFrame de rechazo
    rejected_df["Salary"].fillna(0, inplace=True)
    
    # Agregar el campo "Causa_Rechazo" según las condiciones
    rejected_df["Causa_Rechazo"] = ""
    rejected_df.loc[rejected_df["Employee_Key"].isnull() | rejected_df["Employee_Key"].eq(""), "Causa_Rechazo"] = "Sin Identificador de empleado"
    rejected_df.loc[rejected_df["Dealership_Manager"].isnull() | rejected_df["Dealership_Manager"].eq(""), "Causa_Rechazo"] = "Sin Identificador de distribuidor"
    rejected_df.loc[rejected_df["Salary"].isnull(), "Causa_Rechazo"] = "Sin salario"

    # Eliminar registros de rechazo del DataFrame principal
    merged_df = merged_df.drop(rejected_df.index)

    create_folder(only_date)

    # Seleccionar campos relevantes
    merged_df = merged_df[["Employee_Key", "Name", "Phone_Number", "Fax_Number", "Email", "Address", "Salary", "Dealership_Manager", "Position_Type", "City", "State", "Region", "Country"]]
    
    # Guardar el resultado en archivo CSV
    merged_df.to_csv(os.path.join(current_dir, "../results/final/dim_employees_accepted.csv"), index=False, mode="w")
    merged_df.to_csv(os.path.join(current_dir, f"../results/rollback/{only_date}/dim_employees_accepted_{current_time}.csv"), index=False, mode="w")

    # Guardar el resultado de rechazo como archivo CSV
    rejected_df = rejected_df[["Employee_Key", "Name", "Phone_Number", "Fax_Number", "Email", "Address", "Salary", "Dealership_Manager", "Position_Type", "City", "State", "Region", "Country","Causa_Rechazo"]]
    rejected_df.to_csv(os.path.join(current_dir, "../results/final/dim_employees_rejected.csv"), index=False, mode="w")
    rejected_df.to_csv(os.path.join(current_dir, f"../results/rollback/{only_date}/dim_employees_rejected_{current_time}.csv"), index=False, mode="w")

# Formatear Phone_Number y Fax_Number
def format_phone(phone):
    if not pd.isnull(phone):
        phone_str = str(int(phone))  # Convierte el número en una cadena sin ceros a la izquierda
        return f"({phone_str[:3]}) {phone_str[3:6]}-{phone_str[6:]}"
    return phone  # Mantener nulos si corresponde

def create_folder(only_date):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    directory = f"../results/rollback/{only_date}"
    # Crea el directorio si no existe
    
    if not os.path.exists(os.path.join(current_dir, directory)):
        os.makedirs(os.path.join(current_dir, directory))

#if __name__ == "__main__":
    #main()
