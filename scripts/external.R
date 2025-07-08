library(dplyr)
library(stringr)
library(purrr)

files <- c("/Volumes/acasis/worms/WoRMS_OBIS/identifier.txt", "/Volumes/acasis/worms/WoRMS_DwC-A/identifier.txt")

df <- map(files, function(file) {
  read.table(file, sep = "\t", header = TRUE) %>% 
    filter(datasetID == "ncbi") %>% 
    mutate(
      id = str_extract(taxonID, "\\d+"),
      ncbi_id = str_extract(identifier, "\\d+"),
      bold_id = NA
    ) %>% 
    select(id, bold_id, ncbi_id)
}) %>% 
  bind_rows() %>% 
  distinct()

write.table(df, "data/external.tsv", sep = "\t", row.names = FALSE, na = "", quote = FALSE)
