library(readfl)
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


# -----------------------------------------INITIAL DATA IMPORT WORK ----------------------------------------------
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

############################################################3
##shiprocket zone_e orders discrepancy check
shprkt_zone_e <- shprkt[(shprkt$Zone == "z_e"),]
shprkt_zone_e$order_date <- as.Date(shprkt_zone_e$Date..Time, format="%m/%d/%Y %H:%M")

#added rs.2 for orders picked up after date 12th sep
shprkt_zone_e <- shprkt_zone_e %>%
  mutate(
    # Extract date from datetime
    order_date = as.Date(Date..Time, format = "%m/%d/%Y %H:%M"),
    
    # Add +2 before 12 Sep 2025
    Amt_plus2 = case_when(
      order_date < as.Date("9/12/2025", format = "%m/%d/%Y") ~ Freight.Total.Amount + 2,
      order_date >= as.Date("9/12/2025", format = "%m/%d/%Y")  ~ Freight.Total.Amount
    )
  )


df_zone_e <- list()
weights <- as.character(unique(shprkt_zone_e$Weight_Category))
couriers <- as.character(unique(shprkt_zone_e$Courier.Name))
zones <- as.character(unique(shprkt_zone_e$Zone))
payment_method <- as.character(unique(shprkt_zone_e$Payment.Mode))

for (x in 1:length(couriers)) {
  holder <- shprkt_zone_e[shprkt_zone_e$Courier.Name == couriers[x],]
  temp_payment <- unique(holder$Payment.Mode)
  for (y in 1:length(temp_payment)) {
    holder1 <- holder[holder$Payment.Mode == temp_payment[y],]
    temp_zones <- unique(holder1$Zone)
    for (z in 1:length(temp_zones)) {
      holder2 <- holder1[holder1$Zone == temp_zones[z],]
      temp_weight <- unique(holder2$Weight_Category)
      for (aa in 1:length(temp_weight)) {
        holder3 <- holder2[holder2$Weight_Category == temp_weight[aa],]
        df_zone_e[[length(df_zone_e)+1]] <- holder3
      }
    }
  }
  print(x/length(couriers))
}

#check if the subsets have discrepancy
df_zone_e <- lapply(df_zone_e, function(subset) {
  
  # Calculate adj_total
  subset$adj_total <- round(
    subset$Amt_plus2 -
      subset$Freight.RTO.Amount -
      subset$Freight.Cod.Charges -
      subset$Excess.Total.Amount -
      subset$Freight.Cod.Adjusted, 2
  )
  subset
})

check_freight <- sapply(df_zone_e, function(subset) {
  length(unique(subset$adj_total)) == 1
})



#checking only for rows where value of the order < 2500

check_freight <- sapply(df_zone_e, function(subset) {
  valid_vals <- subset$adj_total[!is.na(subset$Product.Price) & subset$Product.Price <= 2500]
  length(unique(valid_vals)) == 1
})


df_zone_e <- df_zone_e[!sapply(seq_along(df_zone_e), function(i) {
  subset <- df_zone_e[[i]]
  valid_vals <- subset$adj_total[!is.na(subset$Product.Price) & subset$Product.Price <= 2500]
  
  check_freight[i] == FALSE && length(valid_vals) == 1
})]


# Result: TRUE means all values same in that subset, FALSE means not
table(check_freight)
which(!check_freight)
which(check_freight)



df_zone_e_1 <- data.frame(df_zone_e[1])
write.csv(df_zone_e_1, "z_e_order_price_rise_example.csv")

df_zone_e_5 <- data.frame(df_zone_e[5])


#*zone_e has rs 2 added from date 12th sep
#df_zone_e_6 and 9 has no price rise in september 2025
#*all these df_zone_e orders have difference of 0.36 rs and they are all unbilled. (i wonder if there is any specific reason for this pattern)
#df_zone_e_10 has delhivery air order which has difference of 0.36 rs difference for all unbilled orders and applied forward amount rise happened with effect fro 12th sep
#similarly in df_zone_e_11 = delhivery air has 2 rs rise with effect from 12th sep; df_zone_e_12 is also delhivery air orders prepaid.
#*df_zone_e_15 has 2 rs rise, then 0.36 is added for billed as well unbilled orders so i dont understand what is reason for that.
#df_zone_e_16 has all the discrepancy example- rs2 rise in orders after 12th sep; 0.36 added to orders billed as well as unbilled (reason unknown); higher applied forward for PREPAID HIGHER VALUE ORDERS-value above Rs. 2500 
#df_zone_e_17 has discrepancy other than known discrepancies.
#df_zone_e_18, 19, 20, 21, 23, 26, 27, 28,34, 36, 37  has 0.36 added to orders billed as well as unbilled 
#POSSIBLE THEORY: df_zone_e orders have 0.36 increased after date 16th sep MAYBE



#checking if it is general to all orders in zone_e to have 0.36 added to them
#adding 0.36 to orders picked up after 17th sep
shprkt_zone_e <- shprkt_zone_e %>%
  mutate(
    order_date = as.Date(Date..Time, format = "%m/%d/%Y %H:%M"),
    Amt_plus2.36 = case_when(
      order_date <= as.Date("9/17/2025", format = "%m/%d/%Y") ~ Amt_plus2 + 0.36,
      order_date > as.Date("9/17/2025", format = "%m/%d/%Y")  ~ Amt_plus2
    )
  )


df_zone_e <- list()
weights <- as.character(unique(shprkt_zone_e$Weight_Category))
couriers <- as.character(unique(shprkt_zone_e$Courier.Name))
zones <- as.character(unique(shprkt_zone_e$Zone))
payment_method <- as.character(unique(shprkt_zone_e$Payment.Mode))

for (x in 1:length(couriers)) {
  holder <- shprkt_zone_e[shprkt_zone_e$Courier.Name == couriers[x],]
  temp_payment <- unique(holder$Payment.Mode)
  for (y in 1:length(temp_payment)) {
    holder1 <- holder[holder$Payment.Mode == temp_payment[y],]
    temp_zones <- unique(holder1$Zone)
    for (z in 1:length(temp_zones)) {
      holder2 <- holder1[holder1$Zone == temp_zones[z],]
      temp_weight <- unique(holder2$Weight_Category)
      for (aa in 1:length(temp_weight)) {
        holder3 <- holder2[holder2$Weight_Category == temp_weight[aa],]
        df_zone_e[[length(df_zone_e)+1]] <- holder3
      }
    }
  }
  print(x/length(couriers))
}

#check if the subsets have discrepancy
df_zone_e <- lapply(df_zone_e, function(subset) {
  
  # Calculate adj_total
  subset$adj_total <- round(
    subset$Amt_plus2.36 -
      subset$Freight.RTO.Amount -
      subset$Freight.Cod.Charges -
      subset$Excess.Total.Amount -
      subset$Freight.Cod.Adjusted, 2
  )
  subset
})

check_freight <- sapply(df_zone_e, function(subset) {
  length(unique(subset$adj_total)) == 1
})



#checking only for rows where value of the order < 2500

check_freight <- sapply(df_zone_e, function(subset) {
  valid_vals <- subset$adj_total[!is.na(subset$Product.Price) & subset$Product.Price <= 2500]
  length(unique(valid_vals)) == 1
})


df_zone_e <- df_zone_e[!sapply(seq_along(df_zone_e), function(i) {
  subset <- df_zone_e[[i]]
  valid_vals <- subset$adj_total[!is.na(subset$Product.Price) & subset$Product.Price <= 2500]
  
  check_freight[i] == FALSE && length(valid_vals) == 1
})]


# Result: TRUE means all values same in that subset, FALSE means not
table(check_freight)
which(!check_freight)
which(check_freight)

#finding: some subset had 0.36 rs increased for their orders after 16th sep and some have 0.36 increased after 17th sep
