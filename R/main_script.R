# Instalar paquetes necesarios si faltan
paquetes <- c("data.table", "purrr", "furrr", "sf", "parallel", "arrow", "dplyr")
install.packages(paquetes[!(paquetes %in% installed.packages()[, "Package"])])

# Cargar librerías
library(sf)
library(dplyr)
library(furrr)
library(purrr)
library(data.table)
library(parallel)

# Configurar paralelización (mínimo 2 núcleos)
plan(multisession, workers = max(2, detectCores() - 1))

# Definir ruta absoluta a la carpeta de datos para evitar problemas de working directory
data_dir <- "C:/Users/Filosofia/Documents/GitHub/big_data_training/raw_data"

# Verificar que la carpeta exista y tenga archivos .gpkg
if (!dir.exists(data_dir)) {
  stop("La carpeta 'raw_data' no existe en la ruta: ", data_dir)
}

file_list <- list.files(data_dir, pattern = "\\.gpkg$", full.names = TRUE)

if (length(file_list) == 0) {
  stop("No se encontraron archivos .gpkg en la carpeta 'raw_data'")
}

# Leer un archivo que contenga el polígono de Zaragoza (el primero de la lista)
zaragoza_shape <- st_read(file_list[1])

# Unir en un solo polígono si hay varios
zaragoza_union <- st_union(zaragoza_shape)

# Obtener CRS del primer archivo (asumiendo que todos comparten CRS)
example_crs <- st_crs(st_read(file_list[1], quiet = TRUE))

# Reproyectar el polígono de Zaragoza al CRS de los datos
zaragoza_union <- st_transform(zaragoza_union, example_crs)

# Extraer WKT del polígono para filtrado espacial
wkt_zaragoza <- st_as_text(zaragoza_union)

# Función para procesar un archivo individual
procesar_archivo <- function(filepath) {
  tryCatch({
    message("Procesando archivo: ", filepath)
    
    # Leer solo datos dentro del polígono de Zaragoza usando wkt_filter
    datos <- read_sf(filepath, wkt_filter = wkt_zaragoza)
    
    message("Filas leídas: ", nrow(datos))
    
    # Verificar si existen las columnas necesarias
    vars <- c("temperature", "precipitation", "humidity", "datetime")
    if (!all(vars %in% names(datos))) {
      message("Faltan columnas en ", basename(filepath), ". Columnas requeridas: ", paste(vars, collapse = ", "))
      return(NULL)
    }
    
    # Calcular medias
    resumen <- datos %>%
      summarise(
        mean_temp = mean(temperature, na.rm = TRUE),
        mean_precip = mean(precipitation, na.rm = TRUE),
        mean_humidity = mean(humidity, na.rm = TRUE)
      ) %>%
      mutate(date = as.Date(unique(datos$datetime)))
    
    return(resumen)
    
  }, error = function(e) {
    message("Error procesando ", basename(filepath), ": ", e$message)
    return(NULL)
  })
}

# Procesar todos los archivos en paralelo
resultados <- future_map_dfr(file_list, procesar_archivo, .progress = TRUE)

# Guardar resultados en CSV
fwrite(resultados, "C:/Users/Filosofia/Documents/GitHub/big_data_training/results/resultados_zaragoza.csv")

# Mostrar resultados en consola
print(resultados)
