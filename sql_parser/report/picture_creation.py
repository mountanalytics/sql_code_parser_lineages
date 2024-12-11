####### Figures for the report

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
DIR = 'C:/Users/ErwinSiegers/Documents/GitHub/sas_code_parser/report/data/'
Path = f'{DIR}SAP_Summary.csv'

Summary = pd.read_csv(Path, delimiter=';')

x = Summary.DatabaseName
y = Summary.ElementCount
z = Summary.TableCount

X_axis = np.arange(len(x))


#graph colors
ElementColor = ('#D1B07C')
TableColor = ('#758D99')

# function to add value labels

def addlabels(yvalue1,yvalue2, xvalue):
    for i in range(len(xvalue)):
        plt.text(i - 0.2, yvalue1[i]/2,yvalue1[i], ha = 'center', bbox = dict(facecolor = 'white', alpha = .3))
        plt.text(i + 0.2, yvalue2[i]/2,yvalue2[i], ha = 'center', bbox = dict(facecolor = 'white', alpha = .3))



plt.figure()    
plt.bar(X_axis - 0.2, y, 0.4, label = 'Element Count', color = ElementColor)
plt.bar(X_axis + 0.2, z, 0.4, label = 'Table Count', color = TableColor)
plt.xticks(X_axis, x)
plt.xticks(fontsize = 7)
addlabels(y, z, x)
plt.legend(['Element Count', 'Table Count'])
plt.xlabel('Database Name')
plt.ylabel('Count')
#plt.title('Transformation Count per Report')
plt.savefig(f"{DIR}Table&ElementCount.jpeg", dpi = 150)
plt.close()

