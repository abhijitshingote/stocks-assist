"""
SIC Code Mapper for Industry and Sector Classification
"""

# SIC Code to Industry/Sector mapping
SIC_MAPPING = {
    # Division A: Agriculture, Forestry, And Fishing
    '0100': {'sector': 'Agriculture', 'industry': 'Agricultural Production - Crops'},
    '0200': {'sector': 'Agriculture', 'industry': 'Agricultural Production - Livestock'},
    '0700': {'sector': 'Agriculture', 'industry': 'Agricultural Services'},
    '0900': {'sector': 'Agriculture', 'industry': 'Fishing, Hunting, and Trapping'},

    # Division B: Mining
    '1000': {'sector': 'Mining', 'industry': 'Metal Mining'},
    '1200': {'sector': 'Mining', 'industry': 'Coal Mining'},
    '1300': {'sector': 'Mining', 'industry': 'Oil and Gas Extraction'},
    '1400': {'sector': 'Mining', 'industry': 'Nonmetallic Minerals, Except Fuels'},

    # Division C: Construction
    '1500': {'sector': 'Construction', 'industry': 'Building Construction'},
    '1600': {'sector': 'Construction', 'industry': 'Heavy Construction'},
    '1700': {'sector': 'Construction', 'industry': 'Special Trade Contractors'},

    # Division D: Manufacturing
    '2000': {'sector': 'Manufacturing', 'industry': 'Food and Kindred Products'},
    '2100': {'sector': 'Manufacturing', 'industry': 'Tobacco Products'},
    '2200': {'sector': 'Manufacturing', 'industry': 'Textile Mill Products'},
    '2300': {'sector': 'Manufacturing', 'industry': 'Apparel and Other Textile Products'},
    '2400': {'sector': 'Manufacturing', 'industry': 'Lumber and Wood Products'},
    '2500': {'sector': 'Manufacturing', 'industry': 'Furniture and Fixtures'},
    '2600': {'sector': 'Manufacturing', 'industry': 'Paper and Allied Products'},
    '2700': {'sector': 'Manufacturing', 'industry': 'Printing and Publishing'},
    '2800': {'sector': 'Manufacturing', 'industry': 'Chemicals and Allied Products'},
    '2900': {'sector': 'Manufacturing', 'industry': 'Petroleum and Coal Products'},
    '3000': {'sector': 'Manufacturing', 'industry': 'Rubber and Miscellaneous Plastics Products'},
    '3100': {'sector': 'Manufacturing', 'industry': 'Leather and Leather Products'},
    '3200': {'sector': 'Manufacturing', 'industry': 'Stone, Clay, and Glass Products'},
    '3300': {'sector': 'Manufacturing', 'industry': 'Primary Metal Industries'},
    '3400': {'sector': 'Manufacturing', 'industry': 'Fabricated Metal Products'},
    '3500': {'sector': 'Manufacturing', 'industry': 'Industrial Machinery and Equipment'},
    '3600': {'sector': 'Manufacturing', 'industry': 'Electronic and Other Electrical Equipment'},
    '3700': {'sector': 'Manufacturing', 'industry': 'Transportation Equipment'},
    '3800': {'sector': 'Manufacturing', 'industry': 'Measuring, Analyzing, and Controlling Instruments'},
    '3900': {'sector': 'Manufacturing', 'industry': 'Miscellaneous Manufacturing Industries'},

    # Division E: Transportation, Communications, Electric, Gas, And Sanitary Services
    '4000': {'sector': 'Transportation', 'industry': 'Railroad Transportation'},
    '4100': {'sector': 'Transportation', 'industry': 'Local and Suburban Transit'},
    '4200': {'sector': 'Transportation', 'industry': 'Motor Freight Transportation'},
    '4400': {'sector': 'Transportation', 'industry': 'Water Transportation'},
    '4500': {'sector': 'Transportation', 'industry': 'Transportation by Air'},
    '4600': {'sector': 'Transportation', 'industry': 'Pipelines, Except Natural Gas'},
    '4700': {'sector': 'Transportation', 'industry': 'Transportation Services'},
    '4800': {'sector': 'Communications', 'industry': 'Communications'},
    '4900': {'sector': 'Utilities', 'industry': 'Electric, Gas, and Sanitary Services'},

    # Division F: Wholesale Trade
    '5000': {'sector': 'Wholesale Trade', 'industry': 'Wholesale Trade - Durable Goods'},
    '5100': {'sector': 'Wholesale Trade', 'industry': 'Wholesale Trade - Non-durable Goods'},

    # Division G: Retail Trade
    '5200': {'sector': 'Retail Trade', 'industry': 'Building Materials and Garden Supplies'},
    '5300': {'sector': 'Retail Trade', 'industry': 'General Merchandise Stores'},
    '5400': {'sector': 'Retail Trade', 'industry': 'Food Stores'},
    '5500': {'sector': 'Retail Trade', 'industry': 'Automotive Dealers and Service Stations'},
    '5600': {'sector': 'Retail Trade', 'industry': 'Apparel and Accessory Stores'},
    '5700': {'sector': 'Retail Trade', 'industry': 'Home Furniture and Equipment Stores'},
    '5800': {'sector': 'Retail Trade', 'industry': 'Eating and Drinking Places'},
    '5900': {'sector': 'Retail Trade', 'industry': 'Miscellaneous Retail'},

    # Division H: Finance, Insurance, And Real Estate
    '6000': {'sector': 'Finance', 'industry': 'Depository Institutions'},
    '6100': {'sector': 'Finance', 'industry': 'Non-depository Credit Institutions'},
    '6200': {'sector': 'Finance', 'industry': 'Security and Commodity Brokers'},
    '6300': {'sector': 'Insurance', 'industry': 'Insurance Carriers'},
    '6400': {'sector': 'Insurance', 'industry': 'Insurance Agents and Brokers'},
    '6500': {'sector': 'Real Estate', 'industry': 'Real Estate'},
    '6700': {'sector': 'Finance', 'industry': 'Holding and Other Investment Offices'},

    # Division I: Services
    '7000': {'sector': 'Services', 'industry': 'Hotels and Other Lodging Places'},
    '7200': {'sector': 'Services', 'industry': 'Personal Services'},
    '7300': {'sector': 'Services', 'industry': 'Business Services'},
    '7500': {'sector': 'Services', 'industry': 'Automotive Repair, Services, and Parking'},
    '7600': {'sector': 'Services', 'industry': 'Miscellaneous Repair Services'},
    '7800': {'sector': 'Services', 'industry': 'Motion Pictures'},
    '7900': {'sector': 'Services', 'industry': 'Amusement and Recreation Services'},
    '8000': {'sector': 'Services', 'industry': 'Health Services'},
    '8100': {'sector': 'Services', 'industry': 'Legal Services'},
    '8200': {'sector': 'Services', 'industry': 'Educational Services'},
    '8300': {'sector': 'Services', 'industry': 'Social Services'},
    '8400': {'sector': 'Services', 'industry': 'Museums and Other Services'},
    '8600': {'sector': 'Services', 'industry': 'Membership Organizations'},
    '8700': {'sector': 'Services', 'industry': 'Engineering and Management Services'},
    '8800': {'sector': 'Services', 'industry': 'Private Households'},
    '8900': {'sector': 'Services', 'industry': 'Miscellaneous Services'},

    # Division J: Public Administration
    '9100': {'sector': 'Public Administration', 'industry': 'Executive, Legislative, and General Government'},
    '9200': {'sector': 'Public Administration', 'industry': 'Justice, Public Order, and Safety'},
    '9300': {'sector': 'Public Administration', 'industry': 'Public Finance, Taxation, and Monetary Policy'},
    '9400': {'sector': 'Public Administration', 'industry': 'Administration of Human Resource Programs'},
    '9500': {'sector': 'Public Administration', 'industry': 'Administration of Environmental Quality Programs'},
    '9600': {'sector': 'Public Administration', 'industry': 'Administration of Housing Programs'},
    '9700': {'sector': 'Public Administration', 'industry': 'Administration of Economic Programs'},
    '9900': {'sector': 'Public Administration', 'industry': 'National Security and International Affairs'}
}

def get_industry_sector(sic_code):
    """
    Get industry and sector classification based on SIC code.
    
    Args:
        sic_code (str): The SIC code to look up
        
    Returns:
        tuple: (sector, industry) or (None, None) if not found
    """
    if not sic_code:
        return None, None
        
    # Convert to string and ensure 4 digits
    sic_code = str(sic_code).zfill(4)
    
    # Get the first two digits for sector lookup
    sector_code = sic_code[:2] + '00'
    
    # Look up the mapping
    mapping = SIC_MAPPING.get(sector_code)
    if mapping:
        return mapping['sector'], mapping['industry']
    
    return None, None 