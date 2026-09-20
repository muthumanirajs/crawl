"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 358 / 400
================================================================================
- Group: ThirdTest_pincode_group_36_parts_351_to_360
- Assigned PIN Codes: 48 (Range: 781327 to 782102)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_358.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_358.csv & .json
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

PART_ID = "part_358"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-358] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "781327",
  "781328",
  "781329",
  "781330",
  "781333",
  "781334",
  "781335",
  "781337",
  "781338",
  "781339",
  "781340",
  "781341",
  "781343",
  "781344",
  "781346",
  "781347",
  "781348",
  "781349",
  "781350",
  "781351",
  "781352",
  "781353",
  "781354",
  "781355",
  "781360",
  "781364",
  "781365",
  "781366",
  "781367",
  "781368",
  "781369",
  "781370",
  "781371",
  "781372",
  "781373",
  "781374",
  "781375",
  "781376",
  "781377",
  "781378",
  "781380",
  "781381",
  "781382",
  "782001",
  "782002",
  "782003",
  "782101",
  "782102"
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
  "781327": {
    "pincode": "781327",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Jalahghat S.O",
      "Anchali B.O",
      "Bakua B.O",
      "Bamunkata B.O",
      "Betbari B.O",
      "Gandhipur B.O",
      "Golagaon B.O"
    ]
  },
  "781328": {
    "pincode": "781328",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Baghmarabazar S.O",
      "Bhotanta Mohitara B.O",
      "Charnabazar B.O",
      "Kathalmurighat B.O"
    ]
  },
  "781329": {
    "pincode": "781329",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Nityananda S.O",
      "Akaya B.O",
      "Bhogpur B.O",
      "Daisingri B.O",
      "Maripur Anandapur B.O",
      "Pubrihabari B.O",
      "Sarumanikpur B.O",
      "Tuplaipanbari B.O"
    ]
  },
  "781330": {
    "pincode": "781330",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Simla S.O",
      "Barghogra B.O",
      "Koklabari B.O",
      "Kamardisa B.O",
      "Lowkata B.O"
    ]
  },
  "781333": {
    "pincode": "781333",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Barimakha S.O",
      "Bennabari B.O",
      "Dalbari B.O",
      "Dangarmakha B.O",
      "Debachara B.O",
      "Nizbangalipara B.O",
      "Pub Baramchari B.O",
      "Saukuchi B.O",
      "Thanguri B.O"
    ]
  },
  "781334": {
    "pincode": "781334",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Chowkbazar S.O (Nalbari)",
      "Arara B.O",
      "Balilecha B.O",
      "Barazara B.O",
      "Bardigheli B.O",
      "Bhadra B.O",
      "Chandkuchi B.O",
      "Gopalthan B.O",
      "Janigog B.O",
      "Mugkuchi B.O",
      "Niz Bahjani B.O",
      "Terechia B.O"
    ]
  },
  "781335": {
    "pincode": "781335",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Nalbari H.O",
      "Bidyapur S.O",
      "Khatabari S.O"
    ]
  },
  "781337": {
    "pincode": "781337",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Milanpur S.O",
      "Barkhanajan B.O",
      "Chengnoi B.O",
      "Hatinamati B.O",
      "Sondha B.O"
    ]
  },
  "781338": {
    "pincode": "781338",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Solmara S.O",
      "Bori B.O",
      "Helacha B.O",
      "Lakhopur B.O",
      "Niztapa B.O",
      "Piplibari B.O"
    ]
  },
  "781339": {
    "pincode": "781339",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Karia S.O",
      "Arikuchi B.O",
      "Bala B.O",
      "Ulabari B.O"
    ]
  },
  "781340": {
    "pincode": "781340",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Banekuchi S.O",
      "Bakuajari B.O",
      "Khukhundi B.O",
      "Suradi B.O"
    ]
  },
  "781341": {
    "pincode": "781341",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Kazipara S.O",
      "Balikaria B.O",
      "Burinagar B.O",
      "Kendukuchi B.O"
    ]
  },
  "781343": {
    "pincode": "781343",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Dhanbil S.O",
      "Amingaon Chowk B.O",
      "Chaibari B.O",
      "Dighaldonga B.O",
      "Japadang B.O",
      "Pakhamara B.O"
    ]
  },
  "781344": {
    "pincode": "781344",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Baganpara S.O",
      "Angardhowa B.O",
      "Bagulamari B.O",
      "Gerua B.O",
      "Khagrabari B.O",
      "Muktapur B.O",
      "Nayabasti B.O"
    ]
  },
  "781346": {
    "pincode": "781346",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Barama S.O",
      "Barsimlaguri B.O",
      "Dahkaunia B.O",
      "Kaljar B.O",
      "Kathalbari B.O",
      "Kharua B.O",
      "Madhapur B.O",
      "Merkuchi B.O",
      "Murmela B.O"
    ]
  },
  "781347": {
    "pincode": "781347",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Kamarkuchi S.O",
      "Arangmow B.O",
      "Athgharia B.O"
    ]
  },
  "781348": {
    "pincode": "781348",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Marowa S.O",
      "Bagalsroad Chowk B.O",
      "Barkuriha B.O",
      "Budrukuchi B.O",
      "Pubkalakuchi B.O"
    ]
  },
  "781349": {
    "pincode": "781349",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Nizdhamdhama S.O",
      "Balitara B.O",
      "Mahina B.O",
      "Naherbari B.O",
      "Nichalamari B.O",
      "Nizsantipur B.O",
      "Uttarbarsiral B.O"
    ]
  },
  "781350": {
    "pincode": "781350",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Sanekuchi S.O",
      "Balisatra B.O",
      "Barenghati B.O",
      "Chatama B.O",
      "Dakhingaon B.O",
      "Dhuhi B.O",
      "Kamrupbarkhola B.O",
      "Lahkarpara B.O",
      "Sauramari B.O"
    ]
  },
  "781351": {
    "pincode": "781351",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Kalag S.O",
      "Barbari B.O",
      "Baridatara B.O"
    ]
  },
  "781352": {
    "pincode": "781352",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Bhowanipur S.O",
      "Chaparbari Bazar B.O",
      "Galia B.O",
      "Kamalpur B.O",
      "Kayakuchi Bazar B.O",
      "Khablarvitha B.O",
      "Kurobaha B.O",
      "Nakuchi Pathar B.O",
      "Naligaon B.O",
      "Paka Bet Bari Pathar B.O",
      "Sarutapa B.O"
    ]
  },
  "781353": {
    "pincode": "781353",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Gopalbazar S.O",
      "Niz Pakowa B.O",
      "Sandheli B.O"
    ]
  },
  "781354": {
    "pincode": "781354",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Guwahati Division",
    "offices": [
      "Rangia S.O",
      "Bangaon B.O",
      "Bardal B.O",
      "Barghuli B.O",
      "Chapania B.O",
      "Deolkuchi B.O",
      "Dhepargaon B.O",
      "Gopalpur B.O",
      "Kachari Solmari B.O",
      "Madhukuchi B.O",
      "Maranjana B.O",
      "Pitambarhat Bazali B.O",
      "Tarani B.O",
      "Titkuri B.O",
      "Udiana B.O"
    ]
  },
  "781355": {
    "pincode": "781355",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Assam Syntex S.O",
      "Baramchari B.O",
      "Geruapara B.O",
      "Karemura B.O",
      "Medaghat B.O",
      "Nathkuchi B.O",
      "Niz Namati B.O",
      "Sathimuka B.O"
    ]
  },
  "781360": {
    "pincode": "781360",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Kumarikata S.O",
      "Angarkata B.O",
      "Chechapani B.O",
      "Darangamela B.O",
      "Darangapar B.O",
      "Haziragaon B.O",
      "Kalipur B.O",
      "Kauli B.O",
      "Menaka B.O",
      "Paharpur B.O",
      "Pakribari B.O",
      "Patkijuli B.O"
    ]
  },
  "781364": {
    "pincode": "781364",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Guwahati Division",
    "offices": [
      "Maharipara S.O",
      "Chenigaon B.O",
      "Bangalipara B.O",
      "Barpathar B.O",
      "Bholebatabari B.O",
      "Deosunga B.O",
      "Nawkata B.O",
      "Suagpur B.O",
      "Tengajhar B.O",
      "Betagaon B.O"
    ]
  },
  "781365": {
    "pincode": "781365",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Guwahati Division",
    "offices": [
      "Rangia R.S. S.O",
      "Bihapara B.O",
      "Jamtala B.O",
      "Tinali B.O"
    ]
  },
  "781366": {
    "pincode": "781366",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Guwahati Division",
    "offices": [
      "Goreswar S.O",
      "Baghdoba B.O",
      "Bardangrikuchi B.O",
      "Betna B.O",
      "Bhogpur B.O",
      "Gurmow B.O",
      "Karparbhitha B.O",
      "Mahajanpara B.O",
      "Silkijhar B.O"
    ]
  },
  "781367": {
    "pincode": "781367",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Tambulpur S.O",
      "Amayapur B.O",
      "Barkhopa B.O",
      "Dumuria B.O",
      "Gandhibari B.O",
      "Ghogpar B.O",
      "Kachubari B.O",
      "Kachukata B.O",
      "Kalakuchi B.O",
      "Kalbari B.O",
      "Mazdia B.O",
      "Natunhowly B.O",
      "Nizdafeli B.O",
      "Pachimjargaon B.O",
      "Paddapar B.O",
      "Tinpukhuri B.O",
      "Ulubari B.O",
      "Zarkona B.O"
    ]
  },
  "781368": {
    "pincode": "781368",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Nagrijuli S.O",
      "Bagrikhuti B.O",
      "Dangargaon B.O",
      "Dhekipota B.O",
      "Dongpar B.O",
      "Kalcheni B.O",
      "Latibari B.O",
      "Sidhinathpur B.O",
      "Tebitola B.O"
    ]
  },
  "781369": {
    "pincode": "781369",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Ghograpar S.O",
      "Allia B.O",
      "Barajol B.O",
      "Chilling B.O",
      "Guakuchi B.O",
      "Khatikuchi B.O",
      "Nankarbhaira B.O"
    ]
  },
  "781370": {
    "pincode": "781370",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Kaithalkuchi S.O",
      "Gobradal B.O",
      "Kharsitha B.O"
    ]
  },
  "781371": {
    "pincode": "781371",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Tihu S.O",
      "Bhaluki B.O",
      "Digheli B.O",
      "Nakhara B.O",
      "Sialmari B.O"
    ]
  },
  "781372": {
    "pincode": "781372",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Mussalpur S.O",
      "Adalbari B.O",
      "Banbari B.O",
      "Barimukh B.O",
      "Dihira B.O",
      "Kataligaon B.O",
      "Motipur B.O",
      "Narayanpur B.O",
      "Nikashi B.O",
      "Pamuapathar B.O",
      "Subankhata B.O",
      "Uttarkuchi B.O"
    ]
  },
  "781373": {
    "pincode": "781373",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Doomni S.O",
      "Ahopa B.O",
      "Barchakadal B.O",
      "Jalagaon B.O"
    ]
  },
  "781374": {
    "pincode": "781374",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Makhibaha S.O",
      "Bhojkuchi B.O",
      "Jalkhana B.O"
    ]
  },
  "781375": {
    "pincode": "781375",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Bangaon S.O (Nalbari)",
      "Golibandha B.O",
      "Helana B.O",
      "Rampur Dekapara B.O"
    ]
  },
  "781376": {
    "pincode": "781376",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Guwahati Division",
    "offices": [
      "Darkuchi S.O",
      "Tulsibari B.O"
    ]
  },
  "781377": {
    "pincode": "781377",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Thamna S.O (Nalbari)",
      "Athiabari B.O",
      "Garamdew B.O",
      "Lafakuchi B.O",
      "Rampur B.O",
      "Charaimari B.O"
    ]
  },
  "781378": {
    "pincode": "781378",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Nalbari Division",
    "offices": [
      "Haribhanga S.O",
      "Bali B.O",
      "Barpit B.O"
    ]
  },
  "781380": {
    "pincode": "781380",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Guwahati Division",
    "offices": [
      "Baihata S.O",
      "Baruajani B.O",
      "Chanmaguri B.O",
      "Dobok B.O",
      "Hinguli B.O",
      "Kalajalchowk B.O",
      "Kaniha B.O",
      "Kharikot B.O",
      "Puthimari B.O"
    ]
  },
  "781381": {
    "pincode": "781381",
    "circle": "Assam circle",
    "region": "N/A",
    "division": "Guwahati Division",
    "offices": [
      "Balikuchi B.O",
      "Bihdia B.O",
      "Karora B.O",
      "Loch B.O",
      "Pingaleswar B.O",
      "Baihata Chariali S.O",
      "Sutargaon B.O"
    ]
  },
  "781382": {
    "pincode": "781382",
    "circle": "Assam Circle",
    "region": "N/A",
    "division": "Guwahati Division",
    "offices": [
      "Soneswar S.O",
      "Bardekpar B.O",
      "Dihina B.O",
      "Helecha B.O",
      "Japia B.O",
      "Khudradimu B.O"
    ]
  },
  "782001": {
    "pincode": "782001",
    "circle": "Assam Circle",
    "region": "Dibrugarh Region",
    "division": "Nagaon Division",
    "offices": [
      "Nagaon H.O",
      "Nagaon Girls  College S.O"
    ]
  },
  "782002": {
    "pincode": "782002",
    "circle": "Assam Circle",
    "region": "Dibrugarh Region",
    "division": "Nagaon Division",
    "offices": [
      "Haiborgaon S.O",
      "Aibheti B.O",
      "Bihlangani B.O",
      "Chillangani B.O",
      "Herapatty B.O",
      "Huzrangagarah B.O",
      "Kachamari Boragaon B.O",
      "Khutikatia B.O",
      "Majarati B.O",
      "Senchowa B.O",
      "Solmari B.O"
    ]
  },
  "782003": {
    "pincode": "782003",
    "circle": "Assam Circle",
    "region": "Dibrugarh Region",
    "division": "Nagaon Division",
    "offices": [
      "Itachali S.O",
      "Chottahaibor B.O",
      "Dakarghat B.O",
      "Dimaruguri B.O",
      "Hatipara B.O",
      "Madhupur B.O",
      "Majpathori B.O",
      "Mohkhuli B.O",
      "Nagaon Padel B.O",
      "Palasani B.O",
      "Pathori B.O",
      "Uriagaon B.O",
      "Amolapatty B.O"
    ]
  },
  "782101": {
    "pincode": "782101",
    "circle": "Assam Circle",
    "region": "Dibrugarh Region",
    "division": "Nagaon Division",
    "offices": [
      "Nonoi S.O",
      "Biharigaon B.O",
      "Oper Uzaragaon B.O",
      "Rantholi B.O",
      "Tulashimukh B.O"
    ]
  },
  "782102": {
    "pincode": "782102",
    "circle": "Assam Circle",
    "region": "Dibrugarh Region",
    "division": "Nagaon Division",
    "offices": [
      "Barhampur S.O",
      "Bamuni B.O",
      "Kandali B.O",
      "Nibukali B.O",
      "Potani Gomothagaon B.O"
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
