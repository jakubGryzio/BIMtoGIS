import csv
class Loader:
    def __init__(self):
        self.records = None

    @staticmethod
    def create_ifc_object(record):
        record_attributes = record[1].split('(', 1)
        label = record_attributes[0]
        attributes = record_attributes[-1][:-2]
        return {record[0]: {'label': label, 'attributes': attributes}}

    def file_loader(self, filename):
        with open(filename, 'r', encoding='UTF-8') as f:
            self.records = [line.rstrip('\n').split('= ') for line in f if '#' in line]

    def ifc_objects_provider(self, csv_path):
        csv_file = open(csv_path, "w", newline='', encoding='utf-8')
        writer = csv.writer(csv_file)
        for record in self.records:
            for key, value in self.create_ifc_object(record).items():
                writer.writerow(["CREATE (:ifcID {"+f"id: '{key}', label: "+f"'{value['label']}', attributes: "+'"'+f"{value['attributes']}"+'"'+"})"])
        csv_file.close()

