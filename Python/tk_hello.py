import Tkinter


def resize(ev=None):
    label.config(font='Helvetica -{} bold'.format(scale.get()))

top = Tkinter.Tk()
top.geometry('500x300')

label = Tkinter.Label(top, text='Hello World!', font='Helvetica -12 bold')
label.pack(fill=Tkinter.Y, expand=1)

scale = Tkinter.Scale(top, from_=10, to=40, orient=Tkinter.HORIZONTAL, command=resize)
scale.set(12)
scale.pack(fill=Tkinter.X, expand=1)

# button = Tkinter.Button(top, text="Quit!", command=top.quit, bg='red')
button = Tkinter.Button(top, text="Quit!", command=top.quit, bg='red', fg='white')
button.pack()

Tkinter.mainloop()