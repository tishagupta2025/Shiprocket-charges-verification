library(dplyr)
library(fuzzyjoin)
library(writexl)
library(jsonlite)
library(rjson)
library(dplyr)
library(httr) 
library(anytime)
library(RPostgres)
library(data.table)
library(RMySQL)
library(readr)


setwd("C:/Users/ACER/OneDrive - Jan Swasthya Technologies Private Limited/Documents/Shiprocket charges validation analysis - aug 2025")

shiprkt_sep_oct <- read.csv("shiprocket_sep-oct 2025.csv")
shiprkt_sep_oct_status <- read.csv("shiprocket_sep-oct 2025_order.csv")
shiprkt_sep_oct <- shiprkt_sep_oct %>%
  mutate(
    RTO_case = case_when(
      Freight.RTO.Amount > 0 ~ TRUE,
      Freight.RTO.Amount == 0 ~ FALSE
    )
  )


shiprocket <- shiprkt_sep_oct[, c("Date..Time" , "Order.Number", "AWB.Number", "Courier.Name", "Payment.Mode" , "Charged.Weight", "Zone", "RTO_case")]
Charges <- read.csv("charge sheet.csv")
Additional_weight_charges <- read.csv("Additional weight charges.csv")
names(Charges)[names(Charges) == "Courier..0me"] <- "Courier.Name"

shiprocket <- merge(
  shiprocket,
  shiprkt_sep_oct_status [, c("Order.ID", "Product.Price")],
  by.x = "Order.Number",
  by.y = "Order.ID",
  all.x = TRUE
)
rm(shiprkt_sep_oct_status)



Charges$Forward <- gsub("^\\?", "", Charges$Forward)
Charges$COD.charge <- gsub("^\\?", "", Charges$COD.charge)
Charges$RTO <- gsub("^\\?", "", Charges$RTO)
Charges$COD.perc <- gsub("[\\?%]", "", Charges$COD.perc)
Charges$RTO[is.na(Charges$RTO)] <- 0

Additional_weight_charges$Forward <- gsub("^\\?", "", Additional_weight_charges$Forward)
Additional_weight_charges$COD.charge <- gsub("^\\?", "", Additional_weight_charges$COD.charge)
Additional_weight_charges$RTO <- gsub("^\\?", "", Additional_weight_charges$RTO)
Additional_weight_charges$COD.perc <- gsub("[\\?%]", "", Additional_weight_charges$COD.perc)


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

shiprocket <- shiprocket[!is.na(shiprocket$Charged.Weight), ]

# Standardize weights (convert to grams)
shiprocket <- shiprocket %>%
  mutate(Weight_g = Charged.Weight * 1000)


###########################################################################################
shiprocket_500 <- shiprocket[shiprocket$Weight_g <= 500, ]
shiprocket_more <- shiprocket[shiprocket$Weight_g > 500,]




#working on shiprocket_500

shiprocket_500 <- merge(shiprocket_500, Charges [, c("Courier.Name", "Zone", "Forward", "COD.charge", "COD.perc", "RTO")], 
                   by.x = c("Courier.Name", "Zone"),
                   by.y = c("Courier.Name", "Zone"),
                   all.x = TRUE)
unique(shiprocket_500$Courier.Name[is.na(shiprocket_500$Forward)])
names(shiprocket_500)[names(shiprocket_500) == "Forward"] <- "Freight_charged"

shiprocket_500 <- shiprocket_500 %>%
  mutate(
    COD.charge = as.numeric(COD.charge),
    COD.perc   = as.numeric(COD.perc),
    RTO = as.numeric(RTO),
    COD.charge = if_else(Payment.Mode == "Prepaid", NA_real_, COD.charge),
    COD.perc   = if_else(Payment.Mode == "Prepaid", NA_real_, COD.perc),
    RTO_charge = if_else(RTO_case == FALSE, NA_real_, RTO),
    COD.perc.Amt = (COD.perc/100) * Product.Price,
    COD_charge = pmax(COD.perc.Amt, COD.charge, na.rm = TRUE),
    COD_charge = round(COD_charge, 2)
  )
shiprocket_500$RTO <- NULL

shiprocket_500 <- merge(shiprocket_500,
                         shiprkt_sep_oct [, c("Order.Number", "Freight.Forward.Amount", "Freight.Cod.Charges", "Freight.RTO.Amount" )],
                         by = "Order.Number",
                         all.x = TRUE)

##########################################
#changing names of the couriers by removing stressed or 5 kg, 2 kg, 10kg etc because we have only prices for courier name, not further bifurcation.
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


#finding additional unit of each weight_g after substracting 500gm
shiprocket_more$extra_units <- ifelse(
  shiprocket_more$Weight_g > 500,
  ceiling((shiprocket_more$Weight_g - 500) / 500),
  0
)

#combine df to additional weight charges data
shiprocket_more <- merge(shiprocket_more, Additional_weight_charges [, c("Courier.Name", "Zone", "Forward", "COD.charge", "COD.perc", "RTO")], 
                        by.x = c("Courier.Name", "Zone"),
                        by.y = c("Courier.Name", "Zone"),
                        all.x = TRUE)
unique(shiprocket_more$Courier.Name[is.na(shiprocket_more$Forward)])

#adding value from charges list for "amazon surface 500gm"
shiprocket_more <- shiprocket_more %>%
  left_join(
    Charges %>% 
      filter(Courier.Name == "Amazon Surface 500gm") %>% 
      select(Courier.Name, Zone, Forward, "COD.charge", "COD.perc", "RTO"),
    by = c("Courier.Name", "Zone")
  ) 

shiprocket_more <- shiprocket_more %>%
  mutate(
    Forward = if_else(!is.na(Forward.y), Forward.y, Forward.x),  # replace only matched rows
    COD.charge = if_else(!is.na(COD.charge.y), COD.charge.y, COD.charge.x),
    COD.perc = if_else(!is.na(COD.perc.y), COD.perc.y, COD.perc.x),
    RTO = if_else(!is.na(RTO.y), RTO.y, RTO.x)
  ) %>%
  select(-Forward.x, -Forward.y, -COD.charge.y, -COD.charge.x, -COD.perc.x, -COD.perc.y, -RTO.x, -RTO.y)



names(shiprocket_more)[names(shiprocket_more) == "Forward"] <- "Additional_charge"

#making rows NA for the non-COD orders for COD charge and RTO charge column NA for non- RTO orders.
#further, RTO charge column name is additional_RTO_charge column because the RTO for each additional 500gm might be different to base 500gm charge
shiprocket_more <- shiprocket_more %>%
  mutate(
    COD.charge = as.numeric(COD.charge),
    COD.perc   = as.numeric(COD.perc),
    RTO = as.numeric(RTO),
    COD.charge = if_else(Payment.Mode == "Prepaid", NA_real_, COD.charge),
    COD.perc   = if_else(Payment.Mode == "Prepaid", NA_real_, COD.perc),
    Additional_RTO_charge = if_else(RTO_case == FALSE, NA_real_, RTO),
    COD.perc.Amt = (COD.perc/100) * Product.Price,
    COD_charge = pmax(COD.perc.Amt, COD.charge, na.rm = TRUE),
    COD_charge = round(COD_charge, 2)
  )
shiprocket_more$RTO <- NULL

#combining actual charged price against calculated prices
shiprocket_more <- merge(shiprocket_more,
                        shiprkt_sep_oct [, c("Order.Number", "Freight.Forward.Amount", "Freight.Cod.Charges", "Freight.RTO.Amount" )],
                        by = "Order.Number",
                        all.x = TRUE)



#combining base charges to this list because it will be used for calculating forward and RTO charge.
shiprocket_more <- merge(shiprocket_more, Charges [, c("Courier.Name", "Zone", "Forward", "RTO")], 
                        by.x = c("Courier.Name", "Zone"),
                        by.y = c("Courier.Name", "Zone"),
                        all.x = TRUE)


#multiplying extra_unit to additional charge and then adding base charge to it for forward charges
shiprocket_more <- shiprocket_more %>% 
  mutate(Additional_charge = as.numeric(gsub("[^0-9\\.]", "", Additional_charge)),
         extra_units = as.numeric(extra_units),
         Forward = as.numeric(Forward),
         Freight_charged =  Forward + (Additional_charge * extra_units))
names(shiprocket_more)[names(shiprocket_more) == "Forward"] <- "Base_charge"

#multiplying extra_unit to additional RTO charge and then adding base RTO charge to it for RTO charges
shiprocket_more <- shiprocket_more %>% 
  mutate(Additional_RTO_charge = as.numeric(gsub("[^0-9\\.]", "", Additional_RTO_charge)),
         RTO = as.numeric(RTO),
         base_RTO = if_else(RTO_case == FALSE, NA_real_, RTO),
         RTO = NULL,
         RTO_charge =  base_RTO + ( Additional_RTO_charge * extra_units))




Freight <- shiprocket_more[, c("Courier.Name", "Zone", "Order.Number", "Date..Time","Payment.Mode","AWB.Number", "Charged.Weight","Product.Price","Weight_g","RTO_case", "Freight_charged", "COD_charge","RTO_charge", "Freight.Forward.Amount", "Freight.Cod.Charges", "Freight.RTO.Amount" )]



Freight -> 
  backup


Freight <- rbind(
  Freight,
  shiprocket_500[, !(names(shiprocket_500) %in% c("COD.charge", "COD.perc", "COD.perc.Amt"))]
)

Freight <- merge(
  Freight,
  shiprkt_sep_oct [, c("Order.Number", "Freight.Cod.Adjusted")],
  by = "Order.Number",
  all.x = TRUE
)

Freight$COD_charge[is.na(Freight$COD_charge)] <- 0
Freight$RTO_charge[is.na(Freight$RTO_charge)] <- 0

Freight <- Freight %>%
  mutate(COD_adjusted = ifelse(RTO_case == TRUE, -COD_charge, 0))



Freight <- Freight %>%
  mutate(
    Freight.Forward.Amount = as.numeric(gsub("[^0-9\\.]", "", Freight.Forward.Amount)),
    Freight_charged = as.numeric(gsub("[^0-9\\.]", "", Freight_charged)),
    COD_charge = as.numeric(gsub("[^0-9\\.]", "",COD_charge)),
    Freight.Cod.Charges = as.numeric(gsub("[^0-9\\.]", "",Freight.Cod.Charges)),
    Total_freight_cal = Freight_charged + COD_charge + COD_adjusted +RTO_charge,
    Total_freight_Shprkt = Freight.Forward.Amount + Freight.Cod.Charges + Freight.Cod.Adjusted + Freight.RTO.Amount,
    difference = Total_freight_Shprkt - Total_freight_cal,
    diff_COD = Freight.Cod.Charges - COD_charge,
    diff_RTO = Freight.RTO.Amount - RTO_charge
  )
