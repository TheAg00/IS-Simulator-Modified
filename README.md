Αλλαγές:
- Προσθήκη αρχείων Improved_MS.py στο φάκελο Algorithms, Moldable_task_model.py στο φάκελο Enviroment και το README.md.
- Ο Improved_MS.py αναπαριστά τον αλγόριθμο 2 απ' το "Online scheduling of moldable parallel tasks". Η εκτέλεσή του εξηγείται αναλυτικά με σχόλια στο αρχείο.
- Ο Moldable_task_model.py αναπαριστά τον αλγόριθμο moldable_task_model από την πτυχιακή του Στράτου σελ 40. Χρησιμοποιείται για τη μετατροπή των εργασιών σε moldable.
- Έγινε προσθήκη του αλγορίθμου στον Scheduler.py στη συνάρτηση set_algorithm και στο main.py στη λίστα με τους υπόλοιπους αλγορίθμους. Ο λόγος που υπάχει Improved_MS_Varaince_LOW και Improved_MS_Varaince_HIGH, είναι για να τρέξει για low και high variance αντίστοιχα.
- Ο Improved_MS αλγόριθμος δεν κάνει προγραμματισμό των εργασιών, αλλά τις κάνει moldable και τις βάζει σε ράφια. Όταν γίνει αυτό, καλούμε τον First Fit για να κάνει τον προγραμματισμό των ραφιών που υπάρχουν οι εργασίες. Για να γίνει αυτό, άλλαξα λίγο τη συνάρτηση run του Scheduler.py.
- Υπάρχει ένα αρχείο png, όπου έτρεξα τον simulator και έκανα screenshot τα αποτελέσματα.
- Στον φάκελο test τεστάρω τον αλγόριθμο με τις τιμές του παραδείγματος του paper "Online scheduling of moldable parallel tasks".