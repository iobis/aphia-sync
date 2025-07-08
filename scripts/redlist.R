library(rredlist)
library(dplyr)
library(arrow)

categories <- c("EX", "EW", "CR", "EN", "VU")
results <- purrr::map(categories, function(category) {
  rredlist::rl_categories(category, key = "GpSVevZHRz1TgWro7K8iKX7LewhjYnmKN1JB", latest = TRUE, scope_code = 1)  
})

df <- purrr::map(results, function(result) {
  result$assessments
}) %>%
  bind_rows() %>% 
  select(species = taxon_scientific_name, category = red_list_category_code) %>% 
  distinct() %>% 
  arrange(species)

# write_parquet(df, "data/redlist.parquet")
write.table(df, "data/redlist.tsv", sep = "\t", row.names = FALSE, na = "", quote = FALSE)
