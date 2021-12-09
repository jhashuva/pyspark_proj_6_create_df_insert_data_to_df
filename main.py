import data_man
import write_data
if __name__ == '__main__':
    count = 0
    while count<3:
        choice = input("1.Record count\n2.Columns\n3. Missing Values\n4. Drop Columns")
        if choice == '1':
            data_man.no_of_records()
        elif choice == '2':
            a = data_man.col_names()
            for i in a:
                print(i)
        elif choice == '3':
            data_man.count()
        elif choice == '4':
            for i in data_man.col_names():
                print(i)
            col_name = input("Enter the column to delete: ")
            a = data_man.delete_cols(col_name)
            for i in a:
                print(i)
            choice = input("Do you want to write to a file ?(YES/NO)")
            if choice=="Yes" or choice =="YES" or choice=="Y":
                write_data.write_data()


        else:
            count = count+1
            print("Try again with correct choice")