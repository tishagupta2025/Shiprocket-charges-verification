install.packages("lubridate")
install.packages("dplyr")
install.packages("readr")
install.packages("tidyr")
install.packages("stringr")
install.packages("ggplot2")
install.packages("purrr")


setwd("C:/Users/ACER/OneDrive - Jan Swasthya Technologies Private Limited/Documents/Shiprocket charges validation analysis - aug 2025")  

library(readxl)
shiprocket <- read_excel("shiprocket_july-aug 2025.csv")
View(shiprocket)

summary(shiprocket)
sum(shiprocket$`Charged Weight` == "NA", na.rm = TRUE)
sum(shiprocket$`Charged Dimensions` == "0", na.rm = TRUE) 

################## Extract date and month from date, time format.################################# ######################################

shiprocket$datetime <- as.POSIXct(shiprocket$`Date, Time`, format="%Y-%m-%d %H:%M:%S")

shiprocket$month_num  <- format(shiprocket$datetime, "%m")
shiprocket$date <- format(shiprocket$datetime, "%d")

################## Calculate 'Total charges'################################# #######################################

library(dplyr)

shiprocket$Total_charge_cal <- rowSums(
  shiprocket[, c("Freight Total Amount", "On Hold Total Amount", "Other Adjustments")],
  na.rm = TRUE
)

################## Extract dimensions recorded by sayacare employee from orders list on the basis of awb################################# ################################
#reason: replace NA values in charged weight list

library(readr)

new_active_orders <- read_csv("orders list/new_active_orders.csv")
past_orders <- read_csv("orders list/past_orders.csv")
cancelled_orders <- read_csv("orders list/cancelled_orders.csv")

# 1. Combine all orders into one dataset
all_orders <- bind_rows(
  new_active_orders %>% select(AWB, dimensions),
  past_orders %>% select(AWB, dimensions),
  cancelled_orders %>% select(AWB, dimensions)
)

# 2. Join with shiprocket on AWB number
shiprocket <- shiprocket %>%
  left_join(all_orders, by = c("AWB Number" = "AWB"))

################## Extract weight from SayaCare recorded Dimension################################# ##################################

library(dplyr)
library(tidyr)

shiprocket <- shiprocket %>%
  separate(`dimensions`, 
           into = c("Length", "Width", "Height", "Weight"), 
           sep = "x",
           convert = TRUE)

shiprocket$`SC volumised Weight`<- ((shiprocket$Length*shiprocket$Width*shiprocket$Height)/5000)

  shiprocket$`SC volumised Weight` <- round(shiprocket$`SC volumised Weight`, 2)

shiprocket$`SC Applied Weight` <- pmax(shiprocket$Weight, shiprocket$`SC volumised Weight`, na.rm = TRUE)
shiprocket$`SC Applied Weight` <- round(shiprocket$`SC Applied Weight`, 2)


################## Create list 'july_aug_costdiff' from original courier dataset################################# ###########################################

library(dplyr)
july_aug_costdiff <- shiprocket %>% select(month_num, date, `AWB Number`, `Courier Name`, `Payment Mode`, `Applied Weight`, `Charged Weight`, Zone, Total_charge_cal, `SC Applied Weight` )

################## Create list 'Charges list' having required dataset for the plot#################################  ##############################
library(dplyr)

charges_list <- july_aug_costdiff %>%
  group_by(`AWB Number`, month_num, date, `Courier Name`, Zone, `Charged Weight`, `Payment Mode`, `Applied Weight`) %>%
  summarise(All_Charges = sum(Total_charge_cal, na.rm = TRUE),
            Total_Orders = n(),
            .groups = "drop")

################## Replacing NA in 'charged weight' with 'Applied weight' values on the basis of AWB number################################# ############################################
library(dplyr)

charges_list <- charges_list %>%
  mutate(`Charged Weight` == "NA",
                                   `Applied Weight`,
                                   `Charged Weight`)


charges_list <- charges_list %>%
  mutate(`Charged Weight` = ifelse(is.na(`Charged Weight`),
                                   `Applied Weight`,
                                   `Charged Weight`))

################## Creating weight category################################################################################################### #################################################

charges_list$`Charged Weight`<- as.numeric(charges_list$`Charged Weight`)
library(dplyr)

charges_list <- charges_list %>%
  mutate(
     Weight_Category = cut(
      `Charged Weight`,
      breaks = seq(0, 8.5, by = 0.5),   # bins from 0 to 8.5
      labels = LETTERS[1:17],           # exactly 17 bins → A–Q
      right = FALSE                     # [a,b)
    )
  )


################## Creating courier service provider categories################################################################## #########################################
library(dplyr)
library(stringr)

charges_list <- charges_list %>%
  mutate(Courier_Group = case_when(
    str_detect(`Courier Name`, regex("Xpressbees", ignore_case = TRUE)) ~ "Xpressbees",
    str_detect(`Courier Name`, regex("Delhivery", ignore_case = TRUE)) ~ "Delhivery",
    str_detect(`Courier Name`, regex("Bluedart|Blue Dart", ignore_case = TRUE)) ~ "Bluedart",
    str_detect(`Courier Name`, regex("Amazon", ignore_case = TRUE)) ~ "Amazon",
    str_detect(`Courier Name`, regex("Ekart", ignore_case = TRUE)) ~ "Ekart",
    str_detect(`Courier Name`, regex("Shadowfax", ignore_case = TRUE)) ~ "Shadowfax",
    str_detect(`Courier Name`, regex("Shree Maruti", ignore_case = TRUE)) ~ "Shree Maruti",
    TRUE ~ "Other"
  ))

class(charges_list)
################## Trial for specific data applying filter manually################################################################## #########################################
#ggplot trials
library(ggplot2)
library(dplyr)

# Filter data
trial1 <- charges_list %>%
  filter(`Payment Mode` == "Prepaid",
         Zone == "z_a",
         Courier_Group == "Xpressbees",
         Weight_Category == "A")
# keep only July & August

# Plot
ggplot(trial1, aes(date, All_Charges, color = factor(month_num), group = month_num )) + geom_line()

################## Creating facet for Zone - a, COD payment################################################################## ###################################################
# Filter data
COD_A <- charges_list %>%
  filter(`Payment Mode` == "COD",
         Zone == "z_a")


ggplot(
  data = COD_A,
  aes(x = date, y = All_Charges, 
      color = factor(month_num), 
      group = month_num)
) +
  geom_line() +
  facet_grid(
    rows = vars(Courier_Group ),   # vertical facets
    cols = vars(Weight_Category)            # horizontal facets
  
  )
str(COD_A)
COD_A <- COD_A %>%
  mutate(`AWB Number` = as.numeric(unlist(`AWB Number`)))
COD_A <- COD_A %>%
  mutate(month_num = as.numeric(unlist(month_num)))
COD_A <- COD_A %>%
  mutate(date = as.numeric(unlist(date)))
COD_A <- COD_A %>%
  mutate(All_Charges = as.numeric(unlist(All_Charges)))

################## Creating facet for Zone - b, COD payment################################################################## #############################################
# Filter data
COD_B <- charges_list %>%
  filter(`Payment Mode` == "COD",
         Zone == "z_b")


ggplot(
  data = COD_B,
  aes(x = date, y = All_Charges, 
      color = factor(month_num), 
      group = month_num)
) +
  geom_line() +
  facet_grid(
    rows = vars(Courier_Group ),   # vertical facets
    cols = vars(Weight_Category),            # horizontal facets
    drop = TRUE
  )
str(COD_B)
COD_B <- COD_B %>%
  mutate(`AWB Number` = as.numeric(unlist(`AWB Number`)))
COD_B <- COD_B %>%
  mutate(month_num = as.numeric(unlist(month_num)))
COD_B <- COD_B %>%
  mutate(date = as.numeric(unlist(date)))
COD_B <- COD_B %>%
  mutate(All_Charges = as.numeric(unlist(All_Charges)))


################## Creating LOOP for facet for Zone - a, COD payment################################################################## ##################################################
library(ggplot2)
library(dplyr)

Zone <- paste0("z_", letters[1:17])   # gives z_a to z_q

for (z in Zone) {
  
  COD_Zone <- charges_list %>%
    filter(`Payment Mode` == "COD", Zone == z) %>%
    mutate(
      `AWB Number` = as.numeric(unlist(`AWB Number`)),
      month_num    = as.numeric(unlist(month_num)),
      date         = as.numeric(date),   # keep as Date if it's a date
      All_Charges  = as.numeric(unlist(All_Charges))
    )
  
  p <- ggplot(
    data = COD_Zone,
    aes(x = date, y = All_Charges,
        color = factor(month_num),
        group = month_num)
  ) +
    geom_line() +
    facet_grid(
      rows = vars(Courier_Group),
      cols = vars(Weight_Category)
    ) 
  
  # show plot
  print(p)
  
  # save plot if you want
  ggsave(filename = paste0("charges_plot_", z, ".png"),
         plot = p, width = 10, height = 6)
}

############################################################################################################################
library(ggplot2)
library(dplyr)

Zone <- paste0("z_", letters[1:17])   # gives z_a to z_q



for (z in Zone) {
  
  COD_Zone <- charges_list %>%
    filter(`Payment Mode` == "Prepaid", Zone == z) %>%
    mutate(
      `AWB Number` = as.numeric(unlist(`AWB Number`)),
      month_num    = as.numeric(unlist(month_num)),
      date         = as.numeric(date)   # keep as Date if it's a date
     
    )
  
  p <- ggplot(
    data = COD_Zone,
    aes(x = date, y = All_Charges,
        color = factor(month_num),
        group = month_num)
  ) +
    geom_line() +
    facet_grid(
      rows = vars(Courier_Group),
      cols = vars(Weight_Category)
    ) 
  
  # show plot
  print(p)
  
  # save plot if you want
  ggsave(filename = paste0("charges_plot_", z, ".png"),
         plot = p, width = 10, height = 6)
}

################## Creating facet for Zone - d, Prepaid payment################################################################## #############################################
# Filter data
library(purrr)

Prepaid_D <- charges_list %>%
  filter(`Payment Mode` == "Prepaid",
         Zone == "z_d")


ggplot(
  data = Prepaid_D,
  aes(x = date, y = All_Charges, 
      color = factor(month_num), 
      group = month_num)
) +
  geom_quantile() +
  facet_grid(
    rows = vars(Courier_Group ),   # vertical facets
    cols = vars(Weight_Category)            # horizontal facets
    
  )

Prepaid_D <- Prepaid_D %>%
  mutate (`AWB Number` = as.numeric(unlist(`AWB Number`)),
         month_num = as.numeric(unlist(month_num)),
         month_num = as.numeric(unlist(month_num)),
        date = as.numeric(unlist(date)),
        All_Charges = as.numeric(unlist(All_Charges))
        )

str(charges_list)
charges_list$`\`Charged Weight\` == "NA"` <- NULL
charges_list$All_Charges <- unlist(charges_list$All_Charges)
