library(readxl)
library(dplyr)
library(anytime)


setwd("C:/Users/ACER/OneDrive - Jan Swasthya Technologies Private Limited/Documents/Shiprocket charges validation analysis - aug 2025")  

shiprocket <- read_excel("shiprocket_july-aug 2025.csv")

################## Extract date and month from date, time format.################################# ######################################

shiprocket$datetime <- as.POSIXct(shiprocket$`Date, Time`, format="%Y-%m-%d %H:%M:%S")
shiprocket$month_num  <- format(shiprocket$datetime, "%m")
shiprocket$date <- format(shiprocket$datetime, "%d")

################### Extracting necessary columns ########################################################################

test1_file <- shiprocket %>% 
  select(month_num, date, `AWB Number`, `Courier Name`, `Payment Mode`, `Applied Weight`, `Freight Forward Amount`, `Freight RTO Amount`, `Freight Cod Charges`, `Freight Cod Adjusted`, `Freight Total Amount`, Zone, `On Hold Forward Amount`, `On Hold RTO Amount`, `On Hold Total Amount`, `Excess Forward Amount`, `Excess RTO Amount`, `Excess Total Amount`, `Charged Weight`) |>
  filter(`Freight Total Amount` != 0)

#Applied is Original
#Excess is Additional
#Freight Total is final

#Applied + Excess = Freight Total
#Excess is mostly from on hold
#For our analysis 

#Applied Weight/Dimensions when no Weight Discrepency
#Charged Weight/Dimensions with Weight Discrepency

##################Extract past orders data from server#################################################################################
past_orders <- dbReadTable(con, "past_orders")


#join total price of the order from past orders to test1_file

test1_file <- test1_file %>%
  left_join(
    past_orders %>% select(AWB, total_amount),
    by = c("AWB Number" = "AWB")
  )

################## Creating weight category################################################################################################### #################################################

test1_file$`Applied Weight`<- as.numeric(test1_file$`Applied Weight`)

test1_file <- test1_file %>%
  mutate(
    Weight_Category = cut(
      `Applied Weight`,
      breaks = seq(0, 8.5, by = 0.5),   # bins from 0 to 8.5
      labels = 500*1:17,           # exactly 17 bins → A–Q
      right = TRUE                     # (a,b]
    )
  )
############################################################################################################################

# Create new table with mismatched price rows

df <- list()
weights <- as.character(unique(test1_file$Weight_Category))
couriers <- as.character(unique(test1_file$`Courier Name`))
zones <- as.character(unique(test1_file$Zone))
payment_method <- as.character(unique(test1_file$`Payment Mode`))

for (x in 1:length(couriers)) {
  holder <- test1_file[test1_file$`Courier Name` == couriers[x],]
  temp_payment <- unique(holder$`Payment Mode`)
  for (y in 1:length(temp_payment)) {
    holder1 <- holder[holder$`Payment Mode` == temp_payment[y],]
    temp_zones <- unique(holder1$Zone)
    for (z in 1:length(temp_zones)) {
      holder2 <- holder1[holder1$Zone == temp_zones[z],]
      temp_weight <- unique(holder2$Weight_Category)
      for (aa in 1:length(temp_weight)) {
        holder3 <- holder2[holder2$Weight_Category == temp_weight[aa],]
        df[[length(df)+1]] <- holder3
      }
    }
  }
  print(x/length(couriers))
}


################################################################################################################

dx <- test1_file[test1_file$`Courier Name` == dx$`Courier Name`[1] & test1_file$Zone == dx$Zone[1] & test1_file$`Payment Mode` == dx$`Payment Mode`[1] & test1_file$Weight_Category == dx$Weight_Category[1],]
####################################################################################################################

# check if "Total Freight Amount" values are identical within each subset


df <- lapply(df, function(subset) {
  subset$adj_total <- round(subset$`Freight Total Amount` -
                             subset$`Freight RTO Amount` -
                             subset$`Freight Cod Adjusted`-
                           subset$`Excess Total Amount`, 2)
  subset
})


check_freight <- sapply(df, function(subset) {
  length(unique(subset$adj_total)) == 1
})

check_freight
# Result: TRUE means all values same in that subset, FALSE means not
table(check_freight)
which(!check_freight)
###############################################################################################################


#find subsets which dont have unique applied forward amount

df <- lapply(df, function(sub) {
  sub$adj_forward <- round(sub$`Freight Forward Amount` -
                             sub$`Excess Forward Amount`, 2)
  sub
})

#####################################################################################################################

check_applied_forward <- sapply(df, function(sub) {
  length(unique(sub$adj_forward)) == 1
})

table(check_applied_forward)
which(!check_applied_forward)
#####################################################################################################################
#to check that all the discrepant subset atleast have one row/order with order price >2500? to prove my theory that the discrepancy is only because of high applied forward amount

# indices of subsets with discrepancy
discrepant_idx <- which(!check_applied_forward)

# check among those whether total_amount > 2500
check_total <- sapply(df[discrepant_idx], function(sub) {
  any(sub$total_amount > 2500)
})

# Show results
table(check_total)

#################################################################################################################
#to check within rows of discrepant subset -  bind discrepant rows then check if those rows have order price>2500.

# 1. Get indices of discrepant subsets
discrepant_idx <- which(!check_applied_forward)

# 2. For each discrepant subset, find the rows that are discrepant
discrepant_rows <- lapply(df[discrepant_idx], function(sub) {
  # Find the "expected" adj_forward (mode / most common value)
  mode_val <- as.numeric(names(sort(table(sub$adj_forward), decreasing = TRUE))[1])
  
  # Mark rows where adj_forward != mode_val
  sub$discrepant_flag <- sub$adj_forward != mode_val
  
  # Keep only discrepant rows
  sub[sub$discrepant_flag, ]
})

# 3. Combine all discrepant rows into one dataframe
all_discrepant_rows <- do.call(rbind, discrepant_rows)

# 4. Now check which of those discrepant rows have total_amount > 2500
over_2500 <- all_discrepant_rows[all_discrepant_rows$total_amount > 2500, ]
under_2500 <- all_discrepant_rows[all_discrepant_rows$total_amount <= 2500, ]

# FYI -- 2 orders in this under_2500 list were in subset list-33 and 80

#################################################################################################################
#Exporting df list with all subsets are separate sheets

wb <- createWorkbook()

for (i in seq_along(df)) {
  addWorksheet(wb, paste0("Sheet", i))
  writeData(wb, sheet = paste0("Sheet", i), df[[i]])
}

saveWorkbook(wb, "shiprocket discrepancy sheet.xlsx", overwrite = TRUE)


#####################################################################################################################

