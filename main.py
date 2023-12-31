import time
import subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from src.transformation import main as runETL #De la clase transformation importar funcion main y la renombre como runETL

# Define la función que deseas ejecutar cuando se detecte un cambio en los archivos CSV
def trigger():
    current_time = time.strftime("%d-%m-%Y %H:%M:%S")
    print(f"Se detectó un cambio en los archivos CSV. {current_time}")
    # Intenta cerrar el archivo si está abierto
    runETL()
    subprocess.run(["python", "main.py"])

# Define la clase que manejará los eventos de cambio en los archivos
class MyHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if event.is_directory:
            return
        if event.src_path.endswith(".csv"):
            time.sleep(3)
            trigger()

if __name__ == "__main__":
    # Ruta a la carpeta que contiene los archivos CSV que deseas monitorear
    directorio = 'assets/'

    # Crea un observador y asócialo con la clase de manejo de eventos
    observer = Observer()
    event_handler = MyHandler()
    observer.schedule(event_handler, path=directorio, recursive=False)

    # Inicia el observador en segundo plano
    observer.start()

    try:
        current_time = time.strftime("%d-%m-%Y %H:%M:%S")
        print(f"Iniciando ejecucion {current_time}")
        while True:
            pass    
    except KeyboardInterrupt:
        # Detén el observador cuando se presione Ctrl+C
        observer.stop()

    # Espera a que el observador finalice
    observer.join()
