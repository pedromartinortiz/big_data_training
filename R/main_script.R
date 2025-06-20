paquetes <- c("data.table", "purrr", "furrr", "sf", "parallel", "arrow")
install.packages(paquetes[!(paquetes %in% installed.packages()[,"Package"])])