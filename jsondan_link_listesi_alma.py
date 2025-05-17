import csv

# Girdi CSV dosyası ve çıkış TXT dosyası
input_csv = r"C:\Users\tursy\market-flyer\aldi_json\aldi_lang.csv"
output_txt = r"C:\Users\tursy\market-flyer\aldi_json\aldi_links.txt"

with open(input_csv, newline='', encoding="utf-8") as csvfile, open(output_txt, "w", encoding="utf-8") as outfile:
    reader = csv.reader(csvfile)
    
    for row in reader:
        if len(row) >= 2:  # En az 2 sütun varsa
            outfile.write(row[1].strip() + "\n")
