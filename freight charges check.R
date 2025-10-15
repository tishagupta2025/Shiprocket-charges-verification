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


setwd("C:/Users/ACER/OneDrive - Jan Swasthya Technologies Private Limited/Documents/Shiprocket charges validation analysis - aug 2025")

shiprkt_sep_oct <- read.csv("shiprocket_sep-oct 2025.csv")
shiprkt_sep_oct_status <- read.csv("shiprocket_sep-oct 2025_order.csv")
shiprkt_sep_oct$Order.Number <- as.character(shiprkt_sep_oct$Order.Number)

shiprocket <- shiprkt_sep_oct[, c("Date..Time" , "Order.Number", "AWB.Number", "Courier.Name", "Payment.Mode" , "Charged.Weight", "Zone")]
Charges <- read.csv("charge sheet.csv")
Additional_weight_charges <- read.csv("Additional weight charges.csv")
names(Charges)[names(Charges) == "Courier..0me"] <- "Courier.Name"


Charges$Forward <- gsub("^\\?", "", Charges$Forward)
Charges$COD.charge <- gsub("^\\?", "", Charges$COD.charge)
Charges$COD.perc <- gsub("[\\?%]", "", Charges$COD.perc)

Additional_weight_charges$Forward <- gsub("^\\?", "", Additional_weight_charges$Forward)
Additional_weight_charges$COD.charge <- gsub("^\\?", "", Additional_weight_charges$COD.charge)
Additional_weight_charges$COD.perc <- gsub("[\\?%]", "", Additional_weight_charges$COD.perc)

Charges$Weight <- c("0.50")
Additional_weight_charges$Weight <- c("0.50")
Charges$Courier.Name[Charges$Courier.Name == "Pikndel Ndd"] <- "Pikndel NDD"
Additional_weight_charges$Courier.Name[Additional_weight_charges$Courier.Name == "Pikndel Ndd"] <- "Pikndel NDD"
shiprocket$Courier.Name[shiprocket$Courier.Name == "Amazon COD Surface 500gm"] <- "Amazon Surface 500gm"
shiprocket$Courier.Name[shiprocket$Courier.Name == "Amazon Prepaid Surface 500g"] <- "Amazon Surface 500gm"
shiprocket$Courier.Name[shiprocket$Courier.Name == "Blue Dart Surface_Stressed"] <- "Blue Dart Surface"
shiprocket$Courier.Name[shiprocket$Courier.Name == "Bluedart Surface - Select  500gm"] <- "Blue Dart Surface"
shiprocket$Courier.Name[shiprocket$Courier.Name == "Delhivery Reverse Surface 5kg"] <- "Delhivery Surface"
shiprocket$Courier.Name[shiprocket$Courier.Name == "Delhivery Reverse Surface"] <- "Delhivery Surface"
shiprocket$Courier.Name[shiprocket$Courier.Name == "Delhivery Surface_Stressed"] <- "Delhivery Surface"
shiprocket$Courier.Name[shiprocket$Courier.Name == "Ekart Logistics Surface_Stressed"] <- "Ekart Logistics Surface"
shiprocket$Courier.Name[shiprocket$Courier.Name == "Shadowfax  Surface_Stressed"] <- "Shadowfax Surface"
shiprocket$Courier.Name[shiprocket$Courier.Name == "Shadowfax Reverse Surface"] <- "Shadowfax Surface"
shiprocket$Courier.Name[shiprocket$Courier.Name == "Xpressbees Reverse Surface"] <- "Xpressbees Surface"
shiprocket$Courier.Name[shiprocket$Courier.Name == "Xpressbees Surface_Stressed"] <- "Xpressbees Surface"

shiprocket <- merge(
  shiprocket,
  shiprkt_sep_oct_status [, c("Order.ID", "Product.Price")],
  by.x = "Order.Number",
  by.y = "Order.ID",
  all.x = TRUE
)





# Standardize weights (convert to grams)
shiprocket <- shiprocket %>%
  mutate(Weight_g = Charged.Weight * 1000)


###########################################################################################
shiprocket_500 <- shiprocket[shiprocket$Weight_g <= 500, ]
shiprocket_more <- shiprocket[shiprocket$Weight_g > 500,]

shiprocket_500 <- merge(shiprocket_500, Charges [, c("Courier.Name", "Zone", "Forward")], 
                   by.x = c("Courier.Name", "Zone"),
                   by.y = c("Courier.Name", "Zone"),
                   all.x = TRUE)
shiprocket_500 <- shiprocket_500[!is.na(shiprocket_500$Charged.Weight), ]
unique(shiprocket_500$Courier.Name[is.na(shiprocket_500$Forward)])
names(shiprocket_500)[names(shiprocket_500) == "Forward"] <- "Freight_charged"

shiprocket_500 <- merge(shiprocket_500,
                         shiprkt_sep_oct [, c("Order.Number", "Freight.Forward.Amount")],
                         by = "Order.Number",
                         all.x = TRUE)

##########################################

shiprocket_more$Courier.Name[shiprocket_more$Courier.Name %in% c("Blue Dart Air")] <- "Blue Dart Air"
shiprocket_more$Courier.Name[shiprocket_more$Courier.Name %in% c("Blue Dart Surface")] <- "Blue Dart Surface "
shiprocket_more$Courier.Name[shiprocket_more$Courier.Name %in% c("Delhivery Surface", "Delhivery Surface 10kg", "Delhivery Surface 2 Kgs", "Delhivery Surface 20kg", "Delhivery Surface 5kg")] <- "Delhivery Surface"
shiprocket_more$Courier.Name[shiprocket_more$Courier.Name %in% c("Ekart Logistics Surface", "Ekart Surface 2kg")] <- "Ekart Logistics Surface"
shiprocket_more$Courier.Name[shiprocket_more$Courier.Name %in% c("Xpressbees Reverse Surface 1kg", "Xpressbees Surface", "Xpressbees Surface 10kg", "Xpressbees Surface 20kg", "Xpressbees Surface 2kg", "Xpressbees Surface 5kg")] <- "Xpressbees Surface "
shiprocket_more$Courier.Name[shiprocket_more$Courier.Name %in% c("Amazon Shipping Surface 1kg", "Amazon Shipping Surface 2kg", "Amazon Shipping Surface 5kg")] <- "Amazon Surface 500gm"
shiprocket_more$Courier.Name[shiprocket_more$Courier.Name %in% c("Shadowfax Heavy 10Kg", "Shadowfax Surface 2Kg")] <- "Shadowfax Surface" 

Additional_weight_charges$Courier.Name <- trimws(Additional_weight_charges$Courier.Name)
shiprocket_more$Courier.Name <- trimws(shiprocket_more$Courier.Name)

Additional_weight_charges$Courier.Name[Additional_weight_charges$Courier.Name == "Courier Name"] <- "Pikndel NDD"



shiprocket_more <- shiprocket_more %>%
  mutate(
    Courier.Name = recode(Courier.Name,
                          "Blue Dart Air" = "Blue Dart Air",
                          "Delhivery Air" = "Delhivery Air",
                          "Delhivery Surface" = "Delhivery Surface",
                          "Pikndel NDD" = "Pikndel NDD",)
  )



shiprocket_more <- shiprocket_more[!is.na(shiprocket_more$Charged.Weight), ]
shiprocket_more$extra_units <- ifelse(
  shiprocket_more$Weight_g > 500,
  ceiling((shiprocket_more$Weight_g - 500) / 500),
  0
)


shiprocket_more <- merge(shiprocket_more, Additional_weight_charges [, c("Courier.Name", "Zone", "Forward")], 
                        by.x = c("Courier.Name", "Zone"),
                        by.y = c("Courier.Name", "Zone"),
                        all.x = TRUE)
unique(shiprocket_more$Courier.Name[is.na(shiprocket_more$Forward)])


names(shiprocket_more)[names(shiprocket_more) == "Forward"] <- "Additional_charge"

shiprocket_more <- merge(shiprocket_more,
      shiprkt_sep_oct [, c("Order.Number", "Freight.Forward.Amount")],
      by = "Order.Number",
      all.x = TRUE)


shiprocket_more <- merge(shiprocket_more, Charges [, c("Courier.Name", "Zone", "Forward")], 
                        by.x = c("Courier.Name", "Zone"),
                        by.y = c("Courier.Name", "Zone"),
                        all.x = TRUE)
shiprocket_more$Additional_charge[shiprocket_more$Courier.Name == "Amazon Surface 500gm"] <- 
  shiprocket_more$Forward[shiprocket_more$Courier.Name == "Amazon Surface 500gm"]
shiprocket_more <- shiprocket_more %>% 
  mutate(Additional_charge = as.numeric(gsub("[^0-9\\.]", "", Additional_charge)),
         extra_units = as.numeric(extra_units),
         Forward = as.numeric(Forward),
         Freight_charged =  Forward + (Additional_charge * extra_units))
names(shiprocket_more)[names(shiprocket_more) == "Forward"] <- "Base_charge"

Freight <- shiprocket_more[, c("Courier.Name", "Zone", "Order.Number", "Date..Time","Payment.Mode","AWB.Number", "Charged.Weight","Product.Price","Weight_g","Freight_charged", "Freight.Forward.Amount" )]

Freight -> 
  backup
Freight <- rbind (Freight, shiprocket_500)

Freight <- Freight %>%
  mutate(
    Freight.Forward.Amount = as.numeric(gsub("[^0-9\\.]", "", Freight.Forward.Amount)),
    Freight_charged = as.numeric(gsub("[^0-9\\.]", "", Freight_charged)),
    difference = Freight.Forward.Amount - Freight_charged
  )
