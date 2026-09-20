"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 351 / 400
================================================================================
- Group: ThirdTest_pincode_group_36_parts_351_to_360
- Assigned PIN Codes: 48 (Range: 764047 to 766003)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_351.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_351.csv & .json
- Concurrency: 16 Workers (High-throughput & resilient)
================================================================================
"""

import os
import sys
import re
import csv
import time
import json
import random
import logging
import urllib.parse
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PART_ID = "part_351"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-351] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "764047",
  "764048",
  "764049",
  "764051",
  "764052",
  "764055",
  "764056",
  "764057",
  "764058",
  "764059",
  "764061",
  "764062",
  "764063",
  "764070",
  "764071",
  "764072",
  "764073",
  "764074",
  "764075",
  "764076",
  "764077",
  "764078",
  "764081",
  "764085",
  "764086",
  "764087",
  "764088",
  "765001",
  "765002",
  "765013",
  "765015",
  "765016",
  "765017",
  "765018",
  "765019",
  "765020",
  "765021",
  "765022",
  "765023",
  "765024",
  "765025",
  "765026",
  "765029",
  "765033",
  "765034",
  "766001",
  "766002",
  "766003"
]

# 256 Unique Business Categories
CATEGORIES = [
  "Kirana Store",
  "Supermarket",
  "Departmental Store",
  "Provision Store",
  "Organic Food Store",
  "Dairy and Milk Parlour",
  "Fruit and Vegetable Wholesaler",
  "Dry Fruits and Spices Wholesaler",
  "Flour Mill",
  "Edible Oil Wholesaler",
  "Rice and Grain Merchant",
  "Meat and Poultry Shop",
  "Fish Market",
  "General Store",
  "Paan and FMCG Stall",
  "FMCG Distributor",
  "Frozen Food Distributor",
  "Pet Food and Pet Supplies",
  "Sweet Stall / Mithai Shop",
  "Bakery and Cake Shop",
  "Patisserie",
  "Tea Stall / Chai Cafe",
  "Juice Center and Milkshake Bar",
  "Pure Veg Restaurant",
  "Non-Veg Biryani Restaurant",
  "Dhaba and Highway Restaurant",
  "Tiffin Center and Mess",
  "South Indian Restaurant",
  "North Indian Restaurant",
  "Fast Food and Chaat Corner",
  "Cloud Kitchen",
  "Cafe and Coffee Shop",
  "Ice Cream Parlour",
  "Bar and Pub",
  "Family Restaurant",
  "Restaurant Chains",
  "Saree Showroom",
  "Silk Saree Wholesaler",
  "Readymade Garments Shop",
  "Mens Wear Showroom",
  "Womens Ethnic Wear and Kurti",
  "Kids Wear Store",
  "Tailor and Fashion Designer",
  "Textile Wholesaler and Fabric Merchant",
  "Gold and Diamond Jewellery Showroom",
  "Silver Jewellery Shop",
  "Goldsmith and Jewellery Repair",
  "Artificial Jewellery and Accessories",
  "Footwear and Shoe Store",
  "Leather Goods and Bags",
  "Handloom and Khadi Store",
  "Uniform Manufacturer",
  "Bridal Wear and Wedding Collection",
  "Hosiery and Undergarments Wholesaler",
  "Watch Showroom and Repair",
  "Optical Store and Eyewear",
  "Boutiques",
  "Luxury Clothing Shops",
  "Medical Store / Pharmacy",
  "24 Hour Pharmacy",
  "Ayurvedic Pharmacy and Clinic",
  "Homeopathic Clinic",
  "Multispeciality Hospital",
  "Nursing Home and Maternity Hospital",
  "Clinics",
  "Doctors",
  "Dental Clinic",
  "Eye Clinic and Eye Hospital",
  "Skin Clinic and Dermatologist",
  "Pediatrician and Child Clinic",
  "Orthopedic and Physiotherapy Clinic",
  "Diagnostic Center",
  "Pathology Lab and Blood Test",
  "Polyclinic",
  "Dialysis Center",
  "ENT Clinic",
  "Veterinary Clinic and Pet Hospital",
  "Surgical Equipment Supplier",
  "Medical Equipment Supplier",
  "Yoga Center",
  "Gym and Fitness Center",
  "Fitness Chains",
  "Healthcare Clinic Chains",
  "Two Wheeler Repair and Mechanic",
  "Car Repair Workshop and Garage",
  "Car Wash and Auto Detailing",
  "Two Wheeler Showroom and Dealer",
  "Car Showroom and Used Car Dealer",
  "Commercial Vehicle and Tractor Dealer",
  "Auto Spare Parts Shop",
  "Tyre Showroom and Puncture Shop",
  "Car and Bike Battery Dealer",
  "Auto Electrician and AC Repair",
  "CNG Kit Fitment Center",
  "Bicycle Shop and Repair",
  "Taxi Service and Car Rental",
  "Tour and Travel Operator",
  "Bus Booking Agency",
  "Packers and Movers",
  "Logistics and Transport Services",
  "Tempo and Mini Truck Service",
  "Crane and Towing Service",
  "Driving School",
  "Automotive Service Chains",
  "Hardware Store",
  "Electrical Goods and Lighting Store",
  "Sanitaryware and Bathroom Fittings",
  "Paint and Putty Dealer",
  "Tile and Marble Showroom",
  "Granite Dealer",
  "Plywood and Timber Merchant",
  "Glass and Mirror Merchant",
  "Cement and Sand Supplier",
  "TMT Steel and Iron Wholesaler",
  "Building Material Supplier",
  "Borewell Drilling Contractor",
  "Plumber",
  "Electrician",
  "AC Fridge and Washing Machine Repair",
  "RO Water Purifier Sales and Service",
  "Solar Rooftop and Inverter Dealer",
  "Interior Designers",
  "Architects",
  "Civil Contractor and Builder",
  "Roofing Sheet Supplier",
  "False Ceiling Contractor",
  "Waterproofing Contractor",
  "Modular Kitchen Manufacturer",
  "Furniture Showroom",
  "Salon",
  "Beauty Parlour",
  "Spa",
  "Unisex Salon",
  "Bridal Makeup Artist",
  "Cosmetics Wholesaler",
  "Tattoo and Nail Art Studio",
  "Herbal and Ayurvedic Cosmetic Products",
  "Hair Transplant Clinic",
  "Spa Equipment Suppliers",
  "Spa Consultants",
  "Wellness Center",
  "Therapy Center",
  "Marriage Hall / Kalyana Mandapam",
  "Banquet Hall",
  "Event Planners/Wedding Planners",
  "Flower Decorator",
  "Balloon Decorator",
  "Tent House and Shamiana",
  "Sound and Light Rental",
  "Caterer and Event Planner",
  "Photographers",
  "Videographer and Drone Rental",
  "Hotel",
  "Resort",
  "Hostels",
  "PG",
  "Guesthouse",
  "Trousseau Home Decor",
  "Gifting",
  "Cleaning and Hotel Supplier shops/ wholesalers",
  "Hotel Kit Suppliers",
  "Hospitality Consultants",
  "Media and Event",
  "Corporate Event Planner",
  "School",
  "Play School and Daycare",
  "Junior College and Degree College",
  "NEET and JEE Coaching Center",
  "Commerce and CA Coaching",
  "Spoken English Institute",
  "Computer Training Institute",
  "Competitive Exam Coaching (UPSC/Banking)",
  "Tuition Center",
  "Music and Dance Academy",
  "Sports Academy and Turf Ground",
  "Bookstore and Stationery Shop",
  "Educational Consultant",
  "Xerox and Photostat Center",
  "Printing Press and Offset Printer",
  "Flex and Banner Printing",
  "Wedding Invitation Card Printer",
  "Common Service Center (CSC) / E-Seva",
  "Internet Cafe",
  "Computer Sales and Laptop Repair",
  "CCTV Installation and Security System",
  "Mobile Phone Sales and Repair",
  "Mobile Accessories Wholesaler",
  "POS and Billing Software Vendor",
  "Document Writer and Stamp Vendor",
  "IT and Telecom Services",
  "Chartered Accountant (CA)",
  "Tax and GST Consultant",
  "Advocate and Lawyer",
  "Insurance Agent",
  "Home Loan DSA and Loan Consultant",
  "Money Transfer and Forex",
  "Microfinance and NBFC",
  "Pawn Broker and Gold Loan",
  "Chit Fund Company",
  "Stock Broker and Share Sub-broker",
  "Company Registration Consultant",
  "HR Planning and Recruitment",
  "Courier and Cargo Service",
  "Security Guard Agency",
  "Housekeeping Services",
  "Scrap Dealer and Raddi Wholesaler",
  "Financial and Legal Services",
  "Business and Audit Services",
  "Real Estate Agents",
  "Commercial Real Estate Brokerages",
  "Premium Luxury Real Estate",
  "Property Developers",
  "Steel Fabrication Workshop",
  "Welding and Lathe Works",
  "CNC Machining and Laser Cutting",
  "Aluminium Fabrication",
  "Plastic Molding Manufacturer",
  "Corrugated Box and Packaging Material Manufacturers",
  "Chemical Wholesalers",
  "Industrial Hardware and Fasteners",
  "Motor Rewinding and Pump Repair",
  "Generator Sales and Rental",
  "Warehouse and Cold Storage",
  "Rice Mill and Agro Processing",
  "Flour and Oil Mill",
  "Fertilizer and Pesticide Dealer",
  "Agricultural Machinery and Harvester",
  "Industrial Equipment Suppliers",
  "Importers",
  "Exporters",
  "EXIMS",
  "Tradeshows",
  "Exhibitions",
  "Digital Marketing Agencies",
  "Local SEO Agencies",
  "SEO Agencies",
  "SEO Consultants",
  "PPC Advertising Agencies",
  "Social Media Marketing Agencies",
  "Advertisement Agency",
  "Growth Marketing",
  "Lead Generation Agencies",
  "B2B Appointment-Setting Agencies",
  "Telemarketing Firms",
  "SaaS Companies Selling to SMBs",
  "CRM Data Enrichment Companies",
  "Market Research Firms",
  "Malls",
  "Shopping Mall Operators",
  "Multi-location Retail Chains",
  "Commercial Complex",
  "Wholesale Market / Mandi",
  "Industrial Estate / GIDC / MIDC / SIPCOT",
  "Shops",
  "Offices",
  "Businesses"
]

# Pincode to City/Region/Circle Metadata Map
PINCODE_METADATA = {
  "764047": {
    "pincode": "764047",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Kalimela S.O",
      "Bapanpali B.O",
      "Bhejangiwada B.O",
      "Girikanpalli B.O",
      "Gampakonda B.O",
      "Kongurukonda B.O",
      "Manyamkonda B.O",
      "Maranpalli B.O",
      "Motteru B.O",
      "Nallagunthi B.O",
      "Niliguda B.O",
      "Podia B.O",
      "Simlibamchha B.O",
      "Talarai B.O",
      "Venkatpallam B.O"
    ]
  },
  "764048": {
    "pincode": "764048",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Malkangiri Colony S.O"
    ]
  },
  "764049": {
    "pincode": "764049",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Anchalgumma S.O",
      "Amlabhata B.O",
      "Bharanpur B.O",
      "Dhondra B.O",
      "Jhariguma B.O",
      "Kamta B.O",
      "Kontagam B.O",
      "Patraput B.O"
    ]
  },
  "764051": {
    "pincode": "764051",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Balimela S.O",
      "B.Mamidi B.O",
      "Chitapari B.O",
      "Dyke 1 B.O",
      "Kambeda B.O",
      "Muduliguda B.O",
      "Nakamamidi B.O",
      "Nilakhambari B.O",
      "Potrel B.O",
      "Tarlakota B.O",
      "PURUNA CHINTAPALLI B.O."
    ]
  },
  "764052": {
    "pincode": "764052",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Chitrakonda S.O",
      "Andrapali B.O",
      "Badapadar B.O",
      "Boiliguma B.O",
      "Doraguda B.O",
      "Gunthawada B.O",
      "Jodamba B.O",
      "Panasput B.O",
      "Papermetla B.O",
      "Populuru B.O",
      "Relageda B.O",
      "Kurmanur B.O",
      "Kopatuti B.O",
      "Badapoda B.O",
      "Nuaguda B.O"
    ]
  },
  "764055": {
    "pincode": "764055",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Ambaguda S.O",
      "Bobia B.O",
      "Hadia B.O",
      "Haradaput B.O",
      "Jamunda B.O",
      "Konga B.O",
      "Kusumi B.O"
    ]
  },
  "764056": {
    "pincode": "764056",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Borigumma S.O",
      "Anchala B.O",
      "Benasur B.O",
      "Bijapur B.O",
      "Chitra B.O",
      "Gujaniguda B.O",
      "Hardoli B.O",
      "Jayantgiri B.O",
      "Kabidi B.O",
      "Konagam B.O",
      "Kosaguda B.O",
      "Malda B.O",
      "Nuagaon B.O",
      "Panasaguda B.O",
      "Porli B.O",
      "Pujariput B.O",
      "Raniguda B.O",
      "Sanparia B.O",
      "Sasahandi B.O",
      "Sorgiguda B.O"
    ]
  },
  "764057": {
    "pincode": "764057",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Bhairabsinghpur S.O",
      "Benagaon B.O",
      "Bodigaon B.O",
      "Champapadar B.O",
      "Gumuda B.O",
      "Kamta B.O",
      "Kathargada B.O",
      "Kumuli B.O",
      "Munja B.O",
      "Narigam B.O",
      "Ranaspur B.O",
      "Semlaguda B.O",
      "Haridaguda B.O",
      "Duarsuni B.O"
    ]
  },
  "764058": {
    "pincode": "764058",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Kotpad S.O",
      "Batasena B.O",
      "Bhansuli B.O",
      "Binjili B.O",
      "Chandili B.O",
      "Chirma B.O",
      "Dhamanahandi B.O",
      "Ghotarla B.O",
      "Ghumar B.O",
      "Girla B.O",
      "Kerla B.O",
      "Modeigaon B.O",
      "Muttahandi B.O",
      "Sadaranga B.O",
      "Sunarbelli (Nuagaon) B.O",
      "Ukiapalli B.O",
      "Chhatarla B.O",
      "Guali B.O"
    ]
  },
  "764059": {
    "pincode": "764059",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Nabarangpur S.O",
      "Agnipur B.O",
      "Badakumuli B.O",
      "Baghsiuni B.O",
      "Bikrampur B.O",
      "Jambaguda B.O",
      "Kangra B.O",
      "Kukudabai B.O",
      "Kurlughati B.O",
      "Majhiguda B.O",
      "Mantriguda B.O",
      "Taragaon B.O",
      "Nabarangpur Bazar S.O"
    ]
  },
  "764061": {
    "pincode": "764061",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Kosagumuda S.O",
      "Badambeda B.O",
      "Bajraguda B.O",
      "Bhamuni B.O",
      "Ghadadhanua B.O",
      "Kukudisemla B.O",
      "Majhidhanua B.O",
      "Motigaon B.O",
      "Panduguda B.O",
      "Rajoda B.O",
      "Sanameda B.O",
      "Temera B.O"
    ]
  },
  "764062": {
    "pincode": "764062",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Rayagada Division",
    "offices": [
      "Bikrampur S.O",
      "Serjholi B.O"
    ]
  },
  "764063": {
    "pincode": "764063",
    "circle": "Odisha circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Ambadola B.O",
      "Aunli B.O",
      "Bhatrasiuni B.O",
      "Bhatigam B.O",
      "Dengapadar B.O",
      "Jujhari B.O",
      "Porsola B.O",
      "Pujariguda B.O",
      "Rangamatiguda B.O",
      "Sanamasigaon B.O",
      "Sindhigaon B.O",
      "Sindhiguda B.O",
      "Mirganiguda S.O",
      "Baragam B.O"
    ]
  },
  "764070": {
    "pincode": "764070",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Tentulikhunti S.O",
      "Bhejaguda B.O",
      "Charmula B.O",
      "Khuntipadar B.O",
      "Manchagaon B.O",
      "Mentri B.O",
      "Pujariguda B.O"
    ]
  },
  "764071": {
    "pincode": "764071",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Papadahandi S.O",
      "Biriguda B.O",
      "Cherchata B.O",
      "Dengaguda B.O",
      "Hirli B.O",
      "Jatabal B.O",
      "Maidalpur B.O",
      "Majhiguda B.O",
      "Mokiya B.O",
      "Naktiguda B.O",
      "Nuakote B.O",
      "Patri B.O",
      "Pendikote B.O",
      "Semla B.O",
      "Sirisi B.O",
      "Tumberla B.O",
      "Ushigaon B.O"
    ]
  },
  "764072": {
    "pincode": "764072",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Dabugam S.O",
      "Banuaguda B.O",
      "Baigam B.O",
      "Bokadabeda B.O",
      "Borigaon B.O",
      "Chatiguda B.O",
      "Checherguda B.O",
      "Dhamanaguda B.O",
      "Dongriguda B.O",
      "Ghodakhuntia B.O",
      "Jabaguda B.O",
      "Junapani B.O",
      "Koragam B.O",
      "Manigam B.O",
      "Medana B.O",
      "Mohendri B.O",
      "Singisari B.O"
    ]
  },
  "764073": {
    "pincode": "764073",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Umerkote S.O",
      "Adhikariguda B.O",
      "Badabasini B.O",
      "Badabharandi B.O",
      "Badakumari B.O",
      "Beheda B.O",
      "Benara B.O",
      "Bhamini B.O",
      "Bokoda B.O",
      "Burja B.O",
      "Chikalpadar B.O",
      "Dhodra B.O",
      "Hirapur B.O",
      "Jamranda B.O",
      "Khanda B.O",
      "Murtuma B.O",
      "Sirliguda B.O",
      "Sunabeda B.O",
      "Umerkote Bazar S.O"
    ]
  },
  "764074": {
    "pincode": "764074",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Raighar S.O",
      "Bharsundi B.O",
      "Bubei B.O",
      "Debagaon B.O",
      "Ganjapara B.O",
      "Gona B.O",
      "Kumuli B.O",
      "Kurbeda B.O",
      "Mohanda B.O",
      "Naktisimla B.O",
      "Pandripakna B.O",
      "Sarguli B.O",
      "Sonepur B.O"
    ]
  },
  "764075": {
    "pincode": "764075",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Kodinga S.O",
      "Attigaon B.O",
      "B.Gumuda B.O",
      "B.M.Semla B.O",
      "Badaolama B.O",
      "Basini B.O",
      "Belanga B.O",
      "Betal B.O",
      "Chhatahandi B.O",
      "Dongra B.O",
      "Ekori B.O",
      "Jhadakusumi B.O",
      "Kondapuri B.O",
      "Kotagaon B.O",
      "Kutubai B.O"
    ]
  },
  "764076": {
    "pincode": "764076",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Jharigam S.O",
      "Badatemra B.O",
      "Banuaguda B.O",
      "Belagaon B.O",
      "Bhikhya B.O",
      "Chitabeda B.O",
      "Chocha B.O",
      "Ekamba B.O",
      "Palia B.O",
      "Phupugam B.O"
    ]
  },
  "764077": {
    "pincode": "764077",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Chandahandi S.O",
      "Anakabeda B.O",
      "Beheramunda B.O",
      "Dalbeda B.O",
      "Dandamunda B.O",
      "Gambhariguda B.O",
      "Jamadapada B.O",
      "Kolimunda B.O",
      "Melgam B.O",
      "Mohara B.O",
      "Palsapara B.O",
      "Patiki B.O",
      "Patkhalia B.O",
      "Rajkote B.O",
      "Saradhapur B.O"
    ]
  },
  "764078": {
    "pincode": "764078",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Dongerbheja S.O",
      "Digisalpa B.O",
      "Dohona B.O",
      "J.B.Guda B.O",
      "Maliguda B.O",
      "Nandahandi B.O",
      "R.Jaganathpur B.O"
    ]
  },
  "764081": {
    "pincode": "764081",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Lamtaput S.O",
      "Jalahanjar B.O",
      "Soguru B.O",
      "Tikarpada B.O",
      "Tusuba B.O"
    ]
  },
  "764085": {
    "pincode": "764085",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Khatiguda S.O",
      "Parajadeopalli B.O"
    ]
  },
  "764086": {
    "pincode": "764086",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Saradhapalli S.O",
      "Bodili B.O",
      "Chitrangpali B.O",
      "Gurakhunta B.O",
      "Metapoka B.O",
      "Gumuka B.O",
      "Sikhapali B.O"
    ]
  },
  "764087": {
    "pincode": "764087",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Lachipeta S.O",
      "Anantapali B.O",
      "Maharajpalli B.O",
      "Malavaram B.O",
      "Motu B.O",
      "Tandabai B.O",
      "Bhubanpalli B.O"
    ]
  },
  "764088": {
    "pincode": "764088",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Turudihi",
      "Kaudala B.O",
      "Kundei B.O",
      "Jodinga B.O",
      "Khuduku B.O",
      "Deobharandi B.O",
      "Hatabharandi B.O",
      "Jadapara B.O",
      "Parchipara B.O"
    ]
  },
  "765001": {
    "pincode": "765001",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Rayagada Division",
    "offices": [
      "Rayagada(K) H.O",
      "R K Nagar, Rayagada S.O",
      "Rayagada Bazar S.O",
      "Rayagada Gandhinagar S.O",
      "Rayagada College S.O"
    ]
  },
  "765002": {
    "pincode": "765002",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Rayagada Division",
    "offices": [
      "Rayagada S.F. S.O",
      "B.Haluva B.O",
      "Gajigaon B.O",
      "Halva B.O",
      "J.D.Pentho B.O",
      "K.Gumma B.O",
      "Kerada B.O",
      "Kumbhikota B.O",
      "Kutuli B.O",
      "Pitamahal B.O",
      "S.S.Khal B.O",
      "Sulava B.O",
      "Todama B.O"
    ]
  },
  "765013": {
    "pincode": "765013",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Laxmipur (Koraput) S.O",
      "Bilangsil B.O",
      "Burja B.O",
      "Champi B.O",
      "Gadiaguda B.O",
      "Goudaguda B.O",
      "Kakirigumma B.O",
      "Kusumguda B.O",
      "Munjanga B.O",
      "Odiapentha B.O",
      "Panchada B.O",
      "Pipalpadar B.O",
      "Podagada B.O",
      "Toyaput B.O",
      "Upperkutinga B.O"
    ]
  },
  "765015": {
    "pincode": "765015",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Rayagada Division",
    "offices": [
      "Kashipur S.O",
      "Bankamba B.O",
      "Adajori B.O",
      "Bankamba BO",
      "Bilamala B.O",
      "Chandgiri B.O",
      "Chandragiri B.O",
      "Dongasil B.O",
      "Godibali B.O",
      "Gorakhpur B.O",
      "Kampara B.O",
      "Kodipari B.O",
      "Kuchipadar B.O",
      "Maikanch B.O",
      "Mandibisi B.O",
      "Mansugaon B.O",
      "Naktiguda B.O",
      "Panchali B.O",
      "Podapari B.O",
      "Renga B.O",
      "Sankarada B.O",
      "Siripai B.O",
      "Sunger B.O",
      "Tikiri B.O",
      "Turaighati B.O"
    ]
  },
  "765016": {
    "pincode": "765016",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Rayagada Division",
    "offices": [
      "K.Singhpur S.O",
      "D.Panga B.O",
      "Jaganathpur B.O",
      "Korapa B.O",
      "Narayanpur B.O",
      "Polama B.O",
      "Pujariguda B.O",
      "Singari B.O",
      "Sunakhandi B.O"
    ]
  },
  "765017": {
    "pincode": "765017",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Rayagada Division",
    "offices": [
      "J.K.Pur S.O",
      "Antamoda B.O",
      "B.Alubadi B.O",
      "B.Khillapadar B.O",
      "Budaguda B.O",
      "Dangaladi B.O",
      "Dondili B.O",
      "G.Seshkhal B.O",
      "Jilunda B.O",
      "Karubai B.O",
      "Kolnara B.O",
      "Komtalpeta B.O",
      "Kothapeta B.O",
      "Majhiguda B.O",
      "Mirabali B.O",
      "Pipalguda B.O",
      "Sikarpai B.O",
      "Sirigumma B.O",
      "Suri B.O",
      "J.K.Pur Bazar S.O"
    ]
  },
  "765018": {
    "pincode": "765018",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Rayagada Division",
    "offices": [
      "Therubali S.O",
      "Buduni B.O",
      "Dumuriguda B.O",
      "Paikapada B.O"
    ]
  },
  "765019": {
    "pincode": "765019",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Rayagada Division",
    "offices": [
      "Bissamcuttack S.O",
      "B.Gotiguda B.O",
      "Batalpur B.O",
      "Bethiapada B.O",
      "Chatikona B.O",
      "Dukum B.O",
      "Dumurineli B.O",
      "Durgi B.O",
      "H.Donga B.O",
      "Jhigidi B.O",
      "K.Dhamuni B.O",
      "K.Golmi B.O",
      "Kachapai B.O",
      "Kankubadi B.O",
      "Konabai B.O",
      "Kurli B.O",
      "Mundakota B.O",
      "Murtuli B.O",
      "P.Dokuluguda B.O",
      "Patraguda B.O",
      "Penta B.O",
      "Piskapanga B.O",
      "Sahada B.O"
    ]
  },
  "765020": {
    "pincode": "765020",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Rayagada Division",
    "offices": [
      "Muniguda S.O",
      "Agula B.O",
      "B.Bandili B.O",
      "Bhairabguda B.O",
      "Budubali B.O",
      "Chandrapur B.O",
      "Dangasorada B.O",
      "Dohikhal B.O",
      "Hanumanthpur B.O",
      "Hatamuniguda B.O",
      "Jagdalpur B.O",
      "Jarapa B.O",
      "Kumudabali B.O",
      "Kutraguda B.O",
      "Laxmipur B.O",
      "M.Patraguda B.O",
      "T.Padar B.O"
    ]
  },
  "765021": {
    "pincode": "765021",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Rayagada Division",
    "offices": [
      "Ambadola S.O",
      "Ichhapur B.O",
      "Raghubari B.O"
    ]
  },
  "765022": {
    "pincode": "765022",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Rayagada Division",
    "offices": [
      "Gunupur S.O",
      "Chinasari B.O",
      "Godiakhal B.O",
      "Jaltar B.O",
      "Kharlingi B.O",
      "Kulusingi B.O",
      "Puttasingi B.O",
      "Regada B.O",
      "Sagada B.O",
      "Taribili B.O",
      "Tolana B.O",
      "Marathiguda S.O"
    ]
  },
  "765023": {
    "pincode": "765023",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Rayagada Division",
    "offices": [
      "Kujendri S.O",
      "Buthingi B.O",
      "Neelamguda B.O",
      "Omadingi B.O",
      "Panasaguda B.O",
      "Penakham B.O"
    ]
  },
  "765024": {
    "pincode": "765024",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Rayagada Division",
    "offices": [
      "Ukkamba S.O",
      "Bhamini B.O",
      "Bichikote B.O",
      "Derigam B.O",
      "G.Laxmipur B.O",
      "Limapadar B.O",
      "Naira B.O"
    ]
  },
  "765025": {
    "pincode": "765025",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Rayagada Division",
    "offices": [
      "Padmapur S.O",
      "Akhusingi B.O",
      "Ambabadi B.O",
      "Biripadar B.O",
      "Godiabandha B.O",
      "Guluguda B.O",
      "Indupur B.O",
      "Jathili B.O",
      "Karini B.O",
      "Khambariguda B.O",
      "Khilapadar B.O",
      "Khillingrai B.O",
      "Perupanga B.O",
      "Tabalaguda B.O",
      "Tembaguda B.O",
      "Nuaguda B.O"
    ]
  },
  "765026": {
    "pincode": "765026",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Rayagada Division",
    "offices": [
      "Gudari S.O",
      "Asada B.O",
      "Badapankal B.O",
      "Bellamguda B.O",
      "Bijapur B.O",
      "Dhepaguda B.O",
      "Karlaghati B.O",
      "Khariguda B.O",
      "Kinalodi B.O",
      "Kodama B.O",
      "M.Khillingrai B.O",
      "Madhuban B.O",
      "Pendili B.O",
      "Sanahuma B.O",
      "Siriguda B.O"
    ]
  },
  "765029": {
    "pincode": "765029",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Rayagada Division",
    "offices": [
      "Ramanaguda S.O",
      "Bankili B.O",
      "Bhoimada B.O",
      "G.Gulumunda B.O",
      "Gogupada B.O",
      "Gulunthi B.O",
      "Hatikhamba B.O",
      "K.Kota B.O",
      "Katiki B.O",
      "Kinarimada B.O",
      "Kondajam B.O",
      "Panichatra B.O",
      "Rekhapadar B.O",
      "S.Dhamini B.O"
    ]
  },
  "765033": {
    "pincode": "765033",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Rayagada Division",
    "offices": [
      "Dambasora S.O",
      "Bagusala B.O",
      "Bhimpur B.O",
      "Chalakamba B.O",
      "Ghanatri B.O",
      "Khaira B.O",
      "Laba B.O",
      "Tiitmari B.O"
    ]
  },
  "765034": {
    "pincode": "765034",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Rayagada Division",
    "offices": [
      "Jaganathpur S.O",
      "G.Padar B.O",
      "Marma B.O"
    ]
  },
  "766001": {
    "pincode": "766001",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Kalahandi Division",
    "offices": [
      "Bhawanipatna H.O",
      "Bhawanipatna B.B.Pada S.O",
      "Bhawanipatna Collectorate S.O",
      "Bhawanipatna CollegeSquare S.O",
      "Bhawanipatna Naktiguda S.O",
      "Bhawanipatna Stadium S.O"
    ]
  },
  "766002": {
    "pincode": "766002",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Kalahandi Division",
    "offices": [
      "Bhawanipatna G.Chowk S.O",
      "Badbatua B.O",
      "Bundelguda B.O",
      "Dadpur B.O",
      "Deypur B.O",
      "Duarsani B.O",
      "Gudialipadar B.O",
      "Jugsaipatna B.O",
      "Kalam B.O",
      "Kamthana B.O",
      "Karlaguda B.O",
      "Kutrukhamar B.O",
      "Medinipur B.O",
      "Nandol B.O",
      "Nishanpur B.O",
      "Paramanandapur B.O",
      "Risigaon B.O",
      "Sagada B.O",
      "Tikrapara B.O",
      "Uditnarayanpur B.O",
      "Kuliamal B.O"
    ]
  },
  "766003": {
    "pincode": "766003",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Kalahandi Division",
    "offices": [
      "Bhawanipatna Engineering College SO"
    ]
  }
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
]

class SplitPincodeLeadCrawler:
    def __init__(self, max_workers=16):
        self.max_workers = max_workers
        self.session = self._create_resilient_session()
        self.results = []
        self.seen_keys = set()
        self.completed_combos = set()
        self.last_git_push_count = 0
        
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.part_dir = os.path.join(self.script_dir, PART_ID)
        
        # 4 Output Directories
        self.master_dir = os.path.join(self.part_dir, "master")
        self.by_pincode_dir = os.path.join(self.part_dir, "by_pincode")
        self.by_category_dir = os.path.join(self.part_dir, "by_category")
        self.combos_dir = os.path.join(self.part_dir, "by_combination")
        self.ref_dir = os.path.join(self.part_dir, "pincode_city_reference")
        
        for d in [self.master_dir, self.by_pincode_dir, self.by_category_dir, self.combos_dir, self.ref_dir]:
            os.makedirs(d, exist_ok=True)
            
        self.checkpoint_file = os.path.join(self.part_dir, f"checkpoint_{PART_ID}.json")
        self.save_reference_metadata()
        self.load_checkpoint()

    def save_reference_metadata(self):
        try:
            ref_json = os.path.join(self.ref_dir, f"pincode_city_mapping_{PART_ID}.json")
            ref_csv = os.path.join(self.ref_dir, f"pincode_city_mapping_{PART_ID}.csv")
            with open(ref_json, 'w', encoding='utf-8') as f:
                json.dump(PINCODE_METADATA, f, indent=2, ensure_ascii=False)
            with open(ref_csv, 'w', newline='', encoding='utf-8-sig') as f:
                w = csv.DictWriter(f, fieldnames=["pincode", "circle", "region", "division", "offices"])
                w.writeheader()
                for p, meta in PINCODE_METADATA.items():
                    w.writerow({
                        "pincode": meta.get("pincode", p),
                        "circle": meta.get("circle", "N/A"),
                        "region": meta.get("region", "N/A"),
                        "division": meta.get("division", "N/A"),
                        "offices": ", ".join(meta.get("offices", []))
                    })
        except Exception as e:
            logger.warning(f"Could not save reference metadata: {e}")

    def _create_resilient_session(self):
        s = requests.Session()
        retries = Retry(total=5, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries, pool_connections=64, pool_maxsize=64)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        s.headers.update({
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
            "Accept": "*/*",
            "Referer": "https://www.google.com/"
        })
        return s

    def load_checkpoint(self):
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.completed_combos = set(data.get("completed_combos", []))
                    logger.info(f"Loaded checkpoint: {len(self.completed_combos)} combinations already completed.")
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}")

    def save_checkpoint(self):
        try:
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump({"completed_combos": list(self.completed_combos), "updated_at": datetime.now().isoformat()}, f)
        except Exception as e:
            logger.warning(f"Failed to save checkpoint: {e}")

    def _extract_phone(self, details):
        def deep_search(obj):
            if isinstance(obj, str):
                cleaned = obj.strip()
                if re.match(r"^(\+91[\-\s]?)?[0]?(91)?[6789]\d{9}$", cleaned) or (cleaned.startswith("+91") and len(cleaned) >= 13):
                    return cleaned
                if re.match(r"^0\d{2,4}[\-\s]?\d{6,8}$", cleaned):
                    return cleaned
            elif isinstance(obj, list):
                for item in obj:
                    res = deep_search(item)
                    if res:
                        return res
            elif isinstance(obj, dict):
                for v in obj.values():
                    res = deep_search(v)
                    if res:
                        return res
            return None
        found = deep_search(details)
        return found if found else "N/A"

    def _generate_search_angles(self, pincode, category):
        return [
            f"{category} in {pincode}",
            f"Best {category} in {pincode}",
            f"{category} near {pincode}",
            f"{category} dealers suppliers in {pincode}"
        ]

    def git_auto_push_milestone(self, lead_count):
        logger.info("=" * 60)
        logger.info(f"[*] AUTO-SAVE TRIGGERED: {lead_count:,} Leads Scraped! Committing to GitHub...")
        logger.info("=" * 60)
        
        self.export_all()
        self.save_checkpoint()
        
        try:
            repo_root = os.path.abspath(os.path.join(self.script_dir, ".."))
            subprocess.run(["git", "config", "user.name", "github-actions[bot]"], cwd=repo_root, capture_output=True)
            subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], cwd=repo_root, capture_output=True)
            
            rel_part = os.path.relpath(self.part_dir, repo_root)
            subprocess.run(["git", "add", "-A", rel_part], cwd=repo_root, capture_output=True)
            commit_msg = f"Auto-save milestone: {lead_count:,} leads scraped for {PART_ID}"
            subprocess.run(["git", "commit", "-m", commit_msg], cwd=repo_root, capture_output=True)
            
            subprocess.run(["git", "pull", "--rebase", "origin", "main"], cwd=repo_root, capture_output=True)
            push_res = subprocess.run(["git", "push", "origin", "HEAD:main"], cwd=repo_root, capture_output=True, text=True)
            
            if push_res.returncode == 0:
                logger.info(f"[+] SUCCESS: Auto-saved {lead_count:,} leads directly to GitHub repository!")
            else:
                logger.warning(f"[!] Git push notice: {push_res.stderr.strip()}")
        except Exception as git_err:
            logger.warning(f"[!] Git auto-push exception: {git_err}")

    def scrape_single_pair(self, pincode, category):
        combo_key = f"{pincode}_{category}"
        if combo_key in self.completed_combos:
            return []

        leads_for_combo = []
        local_seen = set()
        search_angles = self._generate_search_angles(pincode, category)
        meta = PINCODE_METADATA.get(pincode, {})

        for q in search_angles:
            encoded_q = urllib.parse.quote(q)
            pb_str = (
                f"!1s{encoded_q}!7i20!10b1!12m59!1m5!18b1!30b1!31m1!1b1!34e1!2m4!5m1!6e2!20e3!39b1"
                f"!6m31!32i1!49b1!63m0!66b1!85b1!114b1!149b1!206b1!209b1!212b1!215b1!216b1!222b1!223b1!232b1!234b1!235b1"
                f"!246b1!253b1!260b1!262b1!266b1!270b1!271b1!273b1!280b1!281b1!291m0!294b1!302i300!303i100!10b1!12b1!13b1"
                f"!14b1!16b1!17m1!3e1!20m4!5e2!6b1!8b1!14b1!46m1!1b0!96b1!99b1!19m4!2m3!1i360!2i120!4i8!20m57!2m2!1i0"
                f"!2i20!3m2!2i4!5b1!6m6!1m2!1i86!2i86!1m2!1i408!2i240!7m33!1m3!1e1!2b0!3e3!1m3!1e2!2b1!3e2!1m3!1e2!2b0"
                f"!3e3!1m3!1e8!2b0!3e3!1m3!1e10!2b0!3e3!1m3!1e10!2b1!3e2!1m3!1e10!2b0!3e4!1m3!1e9!2b1!3e2!2b1!9b0!15m8"
                f"!1m7!1m2!1m1!1e2!2m2!1i195!2i195!3i20"
            )
            url = f"https://www.google.com/search?tbm=map&authuser=0&hl=en&gl=in&q={encoded_q}&pb={pb_str}"

            try:
                resp = self.session.get(url, timeout=(3.0, 7.0))
                time.sleep(0.10)

                if resp.status_code == 200:
                    raw_text = resp.text
                    if raw_text.startswith(")]}'"):
                        raw_text = raw_text[raw_text.find('['):]

                    data = json.loads(raw_text)
                    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], list) and len(data[0]) > 1:
                        places_raw = data[0][1]
                        if isinstance(places_raw, list):
                            for p in places_raw:
                                if not isinstance(p, list) or len(p) < 15:
                                    continue
                                d = p[14]
                                if not isinstance(d, list) or len(d) <= 11:
                                    continue

                                name = d[11] if len(d) > 11 and isinstance(d[11], str) else None
                                if not name:
                                    continue

                                place_id = d[78] if len(d) > 78 and d[78] else (d[0] if len(d) > 0 else "N/A")
                                dedup_key = place_id if place_id != "N/A" else f"{name}_{pincode}".lower()

                                if dedup_key in self.seen_keys or dedup_key in local_seen:
                                    continue
                                local_seen.add(dedup_key)
                                self.seen_keys.add(dedup_key)

                                categories_list = d[13] if len(d) > 13 and isinstance(d[13], list) else []
                                primary_category = categories_list[0] if categories_list else category
                                all_categories_str = ", ".join(categories_list) if categories_list else primary_category

                                rating = d[4][7] if len(d) > 4 and isinstance(d[4], list) and len(d[4]) > 7 else None
                                reviews_count = d[4][8] if len(d) > 4 and isinstance(d[4], list) and len(d[4]) > 8 else None

                                website = "N/A"
                                if len(d) > 7 and isinstance(d[7], list) and len(d[7]) > 0 and d[7][0]:
                                    website = str(d[7][0])

                                lat = d[9][2] if len(d) > 9 and isinstance(d[9], list) and len(d[9]) > 2 else None
                                lng = d[9][3] if len(d) > 9 and isinstance(d[9], list) and len(d[9]) > 3 else None

                                address = d[39] if len(d) > 39 and d[39] else (d[18] if len(d) > 18 and d[18] else f"{name}, {pincode}, India")
                                area = d[14] if len(d) > 14 and d[14] else str(pincode)

                                phone = self._extract_phone(d)
                                place_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}" if place_id != "N/A" else "N/A"

                                record = {
                                    "business_name": name,
                                    "search_category": category,
                                    "primary_category": primary_category,
                                    "all_categories": all_categories_str,
                                    "pincode": pincode,
                                    "circle": meta.get("circle", "N/A"),
                                    "region": meta.get("region", "N/A"),
                                    "division": meta.get("division", "N/A"),
                                    "major_offices": ", ".join(meta.get("offices", [])[:3]),
                                    "phone_number": phone,
                                    "website": website,
                                    "rating": rating,
                                    "reviews_count": reviews_count,
                                    "address": address,
                                    "area": area,
                                    "latitude": lat,
                                    "longitude": lng,
                                    "place_id": place_id,
                                    "place_url": place_url,
                                    "part_id": PART_ID,
                                    "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                }
                                leads_for_combo.append(record)
                elif resp.status_code == 429:
                    logger.warning(f"Rate limited on ({pincode}, {category}). Backing off 3s...")
                    time.sleep(3.0)
            except Exception as err:
                logger.debug(f"Notice for ({pincode}, {category}): {err}")

        if leads_for_combo:
            safe_cat = re.sub(r'[^a-zA-Z0-9_]', '_', category).strip('_').lower()
            out_json = os.path.join(self.combos_dir, f"{pincode}_{safe_cat}.json")
            out_csv = os.path.join(self.combos_dir, f"{pincode}_{safe_cat}.csv")
            try:
                with open(out_json, 'w', encoding='utf-8') as f:
                    json.dump(leads_for_combo, f, indent=2, ensure_ascii=False)
                df_c = pd.DataFrame(leads_for_combo)
                df_c.to_csv(out_csv, index=False, encoding='utf-8-sig')
            except Exception as e:
                logger.warning(f"Failed to write combo files: {e}")

        self.completed_combos.add(combo_key)
        return leads_for_combo

    def crawl_all(self):
        all_combinations = [(p, c) for p in ASSIGNED_PINCODES for c in CATEGORIES]
        remaining = [(p, c) for (p, c) in all_combinations if f"{p}_{c}" not in self.completed_combos]
        total_tasks = len(all_combinations)

        logger.info("=" * 60)
        logger.info(f"STARTING CRAWLER PART          : {PART_ID}")
        logger.info(f"Assigned PIN Codes             : {len(ASSIGNED_PINCODES):,}")
        logger.info(f"Target Categories              : {len(CATEGORIES):,}")
        logger.info(f"Total Combinations (Tasks)     : {total_tasks:,}")
        logger.info(f"Remaining Combinations         : {len(remaining):,}")
        logger.info(f"Workers / Concurrency          : {self.max_workers} Threads")
        logger.info("=" * 60)

        completed_count = total_tasks - len(remaining)
        chunk_size = 500

        for chunk_idx in range(0, len(remaining), chunk_size):
            chunk = remaining[chunk_idx:chunk_idx + chunk_size]
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_map = {executor.submit(self.scrape_single_pair, pin, cat): (pin, cat) for pin, cat in chunk}
                for future in as_completed(future_map):
                    pin, cat = future_map[future]
                    completed_count += 1
                    try:
                        records = future.result()
                        if records:
                            self.results.extend(records)
                            logger.info(f"[{completed_count}/{total_tasks}] ({pin} | {cat}) -> Extracted {len(records)} leads | Total: {len(self.results):,} leads")
                            
                            if len(self.results) - self.last_git_push_count >= LEAD_AUTO_SAVE_THRESHOLD:
                                self.last_git_push_count = len(self.results)
                                self.git_auto_push_milestone(len(self.results))
                    except Exception as e:
                        logger.error(f"Error crawling ({pin}, {cat}): {e}")

            self.save_checkpoint()
            if len(self.results) - self.last_git_push_count >= LEAD_AUTO_SAVE_THRESHOLD:
                self.last_git_push_count = len(self.results)
                self.git_auto_push_milestone(len(self.results))

        self.export_all()
        self.git_auto_push_milestone(len(self.results))
        return len(self.results)

    def export_all(self):
        if not self.results:
            logger.warning("No results to export.")
            return

        for idx, item in enumerate(self.results):
            item["s_no"] = idx + 1

        fields = [
            "s_no", "business_name", "search_category", "primary_category", "all_categories",
            "pincode", "circle", "region", "division", "major_offices",
            "phone_number", "website", "rating", "reviews_count",
            "address", "area", "latitude", "longitude", "place_id", "place_url",
            "part_id", "crawled_at"
        ]

        # 1. Master Output (CSV and JSON)
        master_csv = os.path.join(self.master_dir, f"ALL_INDIA_LEADS_{PART_ID.upper()}.csv")
        master_json = os.path.join(self.master_dir, f"ALL_INDIA_LEADS_{PART_ID.upper()}.json")
        df_master = pd.DataFrame(self.results)
        df_master.to_csv(master_csv, index=False, encoding='utf-8-sig')
        with open(master_json, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported Master: {len(self.results):,} leads to CSV and JSON")

        # 2. By Pincode Output (CSV and JSON)
        by_pin = {}
        for r in self.results:
            by_pin.setdefault(str(r.get("pincode")), []).append(r)
        for pin, pin_leads in by_pin.items():
            if not pin: continue
            df_p = pd.DataFrame(pin_leads)
            df_p.to_csv(os.path.join(self.by_pincode_dir, f"{pin}.csv"), index=False, encoding='utf-8-sig')
            with open(os.path.join(self.by_pincode_dir, f"{pin}.json"), 'w', encoding='utf-8') as f:
                json.dump(pin_leads, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported by_pincode: {len(by_pin)} pincode files (both .csv & .json)")

        # 3. By Category Output (CSV and JSON)
        by_cat = {}
        for r in self.results:
            by_cat.setdefault(str(r.get("search_category")), []).append(r)
        for cat, cat_leads in by_cat.items():
            safe_cat = re.sub(r'[^a-zA-Z0-9_]', '_', cat).strip('_').lower()
            df_c = pd.DataFrame(cat_leads)
            df_c.to_csv(os.path.join(self.by_category_dir, f"{safe_cat}.csv"), index=False, encoding='utf-8-sig')
            with open(os.path.join(self.by_category_dir, f"{safe_cat}.json"), 'w', encoding='utf-8') as f:
                json.dump(cat_leads, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported by_category: {len(by_cat)} category files (both .csv & .json)")

def main():
    crawler = SplitPincodeLeadCrawler(max_workers=16)
    crawler.crawl_all()

if __name__ == "__main__":
    main()
