library(dplyr)
library(fuzzyjoin)
library(writexl)
library(jsonlite)
library(rjson)
library(dplyr)
library(httr) #Pulling Data from Firebase
library(anytime)
library(RPostgres)
library(data.table)
library(RMySQL)
library(readr)


con <- dbConnect(RPostgres::Postgres(), 
                 host = "127.0.0.1",
                 port = 3307,
                 dbname = "samasya", 
                 user = "readonly_user", 
                 password = "Careful!Read21")

setwd("C:/Users/ACER/OneDrive - Jan Swasthya Technologies Private Limited/Documents/Shiprocket charges validation analysis - aug 2025")

shiprkt_sep_oct <- read.csv("shiprocket_sep-oct 2025.csv")
shiprkt_sep_oct_status <- read.csv("shiprocket_sep-oct 2025_order.csv")
shiprkt_sep_oct$Order.Number <- as.character(shiprkt_sep_oct$Order.Number)
cancelled_orders <- dbReadTable(con, "cancelled_orders")

cancelled_orders$unique_id <- paste0(gsub("^\\+91", "", cancelled_orders$phone_number), cancelled_orders$order_id)

#identifying reason for NA in charged weight column
remove <- merge(
  shiprkt_sep_oct,
  cancelled_orders [, c("unique_id", "phone_number", "order_id")],
  by.x = "Order.Number",
  by.y = "unique_id",
  all.x = TRUE
) 

remove <- remove %>%
  mutate(
    RTO = case_when(
      Applied.Rto.Amount > 0 ~ TRUE,
      Applied.Rto.Amount == 0 ~ FALSE
    ),
    Cancelled_order = case_when(
      !is.na(phone_number) ~ TRUE,
      is.na(phone_number) ~ FALSE
    )
  )

#removing rows where orders are not RTO yet cancelled
remove <- remove %>%
  filter(!(RTO == FALSE & Cancelled_order == TRUE))

charged_weight_NA <- remove [is.na(remove$Charged.Weight),]
charged_weight_NA<- merge(
  charged_weight_NA,
  shiprkt_sep_oct_status [,c("Order.ID", "Status")],
  by.x = "Order.Number",
  by.y = "Order.ID",
  all.x = TRUE
)

remove <- remove[!is.na(remove$Charged.Weight),]
rm(charged_weight_NA)
#Orders without charged weight are removed from shiprkt_sep_oct
  #reason: order not delivered yet/ order cancelled/ order delivered but not billed to SC

shprkt <- merge(
  remove,
  shiprkt_sep_oct_status [,c("Order.ID", "Status", "Product.Price")],
  by.x = "Order.Number",
  by.y = "Order.ID",
  all.x = TRUE
)
rm(remove
   )


#Applied is Original
#Excess is Additional
#Freight Total is final

#Applied + Excess = Freight Total
#Excess is mostly from on hold
#For our analysis 

#Applied Weight/Dimensions when no Weight Discrepency
#Charged Weight/Dimensions with Weight Discrepency

#creating weight category
shprkt$Applied.Weight<- as.numeric(shprkt$Applied.Weight)

shprkt <- shprkt %>%
  mutate(
    Weight_Category = cut(
      Applied.Weight,
      breaks = seq(0, 8.5, by = 0.5),   # bins from 0 to 8.5
      labels = 500*1:17,           # exactly 17 bins → A–Q
      right = TRUE                     # (a,b]
    )
  )

#analysis of zone_e orders separated so removing z_e orders
shprkt <- shprkt[shprkt$Zone != "z_e",]


#############################################################################################################
#checking for rs 2 added and 0.36
shprkt <- shprkt %>%
  mutate(
    # Extract date from datetime
    order_date = as.Date(Date..Time, format = "%m/%d/%Y %H:%M"),
    
    # Add +2 before 12 Sep 2025
    Amt_plus2 = case_when(
      order_date < as.Date("9/12/2025", format = "%m/%d/%Y") ~ Freight.Total.Amount + 2,
      order_date >= as.Date("9/12/2025", format = "%m/%d/%Y")  ~ Freight.Total.Amount
    )
  )
shprkt <- shprkt %>%
  mutate(
    order_date = as.Date(Date..Time, format = "%m/%d/%Y %H:%M"),
    Amt_plus2.36 = case_when(
      order_date < as.Date("9/17/2025", format = "%m/%d/%Y") ~ Amt_plus2 + 0.36,
      order_date >= as.Date("9/17/2025", format = "%m/%d/%Y")  ~ Amt_plus2 
    )
  )

df <- list()
weights <- as.character(unique(shprkt$Weight_Category))
couriers <- as.character(unique(shprkt$Courier.Name))
zones <- as.character(unique(shprkt$Zone))
payment_method <- as.character(unique(shprkt$Payment.Mode))

for (x in 1:length(couriers)) {
  holder <- shprkt[shprkt$Courier.Name == couriers[x],]
  temp_payment <- unique(holder$Payment.Mode)
  for (y in 1:length(temp_payment)) {
    holder1 <- holder[holder$Payment.Mode == temp_payment[y],]
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


df <- lapply(df, function(subset) {
  subset$adj_total <- round(subset$Amt_plus2.36 -
                              subset$Freight.RTO.Amount -
                              subset$Freight.Cod.Charges-
                              subset$Excess.Total.Amount-
                              subset$Freight.Cod.Adjusted, 2)
  subset
})


check_freight <- sapply(df, function(subset) {
  length(unique(subset$adj_total)) == 1
})


# Result: TRUE means all values same in that subset, FALSE means not
table(check_freight)
which(!check_freight)

##############################################################################
#to check that all the discrepant subset atleast have one row/order with order price >2500? to prove my theory that the discrepancy is only because of high applied forward amount

# indices of subsets with discrepancy
discrepant_idx <- which(!check_freight)

# check among those whether total_amount > 2500
check_total <- sapply(df[discrepant_idx], function(sub) {
  any(sub$Product.Price > 2500)
})

# Show results
table(check_total)
which(!check_total)
