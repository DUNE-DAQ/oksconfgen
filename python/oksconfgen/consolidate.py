import oksdbinterfaces
import sys


def get_all_includes(db, file):
    includes = db.get_includes(file)
    for include in includes:
        if "data.xml" in include:
            includes += get_all_includes(db, include)

    return list(set(includes))


def consolidate_db(oksfile, output_file):
    sys.setrecursionlimit(10000)  # for example
    print("Reading database")
    db = oksdbinterfaces.Configuration("oksconfig:" + oksfile)

    schemafiles = []
    includes = get_all_includes(db, None)
    schemafiles += [i for i in includes if "schema.xml" in i]
    print(f"Included schemas: {schemafiles}")

    print("Creating new database")
    new_db = oksdbinterfaces.Configuration("oksconfig")
    new_db.create_db(output_file, schemafiles)

    new_db.commit()

    print("Reading dal objects from old db")
    dals = db.get_all_dals()

    print(f"Copying objects to new db")
    for dal in dals:

        print(f"Loading object {dal} into cache")
        db.get_dal(dals[dal].className(), dals[dal].id)

        print(f"Copying object: {dal}")
        new_db.add_dal(dals[dal])

    print("Saving database")
    new_db.commit()
    print("DONE")


def consolidate_files(oksfile, *input_files):
    schemafiles = []
    dbs = []

    sys.setrecursionlimit(10000)  # for example

    for input_file in input_files:
        dbs.append(oksdbinterfaces.Configuration("oksconfig:" + input_file))
        includes = get_all_includes(dbs[len(dbs) - 1], None)
        schemafiles += [i for i in includes if "schema.xml" in i]
    schemafiles = list(set(schemafiles))
    print(f"Included schemas: {schemafiles}")

    print("Creating new database")
    new_db = oksdbinterfaces.Configuration("oksconfig")
    new_db.create_db(oksfile, schemafiles)

    new_db.commit()

    for db in dbs:
        # print(f"Reading dal objects from old db")
        dals = db.get_all_dals()

        # print(f"Copying objects to new db")
        for dal in dals:

            # print(f"Loading object {dal} into cache")
            # db.get_dal(dals[dal].className(), dals[dal].id)

            # print(f"Copying object: {dal}")
            new_db.add_dal(dals[dal])

    print("Saving database")
    new_db.commit()
    print("DONE")