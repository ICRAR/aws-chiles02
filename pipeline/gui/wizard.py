#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#
import Tkinter as tk

navigation_button_text_style = ("Helvetica", 10, "italic")

class Wizard:
    """
    The Wizard class manages a set of wizard pages and provides a back and forward navigation between the pages.
    Each wizard page contained within a tk.Frame, and the back and forward buttons are placed at the bottom of the screen.
    """
    def __init__(self, root):
        """
        :param pages: List of pages, in order, that the wizard will sequentially go through
        """
        self.pages = []
        self.current_page = None
        self.current_page_index = 0
        self.on_submit = None
        self.frame = tk.Frame(root)

        navigation = tk.Frame(self.frame, relief=tk.GROOVE, borderwidth=1)
        navigation.pack(side=tk.BOTTOM, fill=tk.X)

        self.previous_button = tk.Button(navigation, text="Previous", font=navigation_button_text_style, command=self.previous_page, width=12)
        self.previous_button.pack(side=tk.LEFT, pady=5, padx=5)

        self.progress_string = tk.StringVar(navigation, "0/0")
        progress = tk.Label(navigation, textvariable=self.progress_string, font=navigation_button_text_style, justify=tk.CENTER)
        progress.pack(side=tk.LEFT, fill=tk.X, expand=True)

        self.next_button = tk.Button(navigation, text="Next", font=navigation_button_text_style, command=self.next_page, width=12)
        self.next_button.pack(side=tk.RIGHT, pady=5, padx=5)

    def __len__(self):
        """
        Number of pages in the wizard
        :return:
        """
        return len(self.pages)

    def set_on_submit(self, callback):
        """
        Set the function to be called when the wizard has gone past the last page
        :param function:
        :return:
        """
        self.on_submit = callback

    def get_page(self, index):
        """
        Get a page from the wizard
        :param index:
        :return:
        """
        return self.pages[index]

    def add_page(self, page):
        """
        Add a page to the wizard
        :return:
        """
        page.frame = tk.Frame(self.frame)
        self.pages.append(page)

        if len(self) == 1:
            self.goto_page(0)
        else:
            self._update_buttons()
            self.progress_string.set("{0}/{1}".format(self.current_page_index + 1, len(self)))

    def next_page(self):
        """
        Go to the next page in the wizard
        :return:
        """
        self.goto_page(self.current_page_index + 1)

    def previous_page(self):
        """
        Go to the previous page in the wizard
        :return:
        """
        self.goto_page(self.current_page_index - 1)

    def goto_page(self, index):
        """
        Go to the specified page in the wizard
        :param index:
        :return:
        """

        if index < 0 or index > len(self):
            return

        new_page = self.pages[index] if index < len(self) else "Submit"
        old_page = self.current_page

        if old_page:
            # Don't leave the old page yet (possibly due to invalid data being on the page
            if not old_page.leave(new_page):
                return
            old_page.frame.pack_forget()

        if new_page == "Submit":
            if self.on_submit is not None:
                self.on_submit(self)
            return

        self.current_page = new_page
        self.current_page_index = index

        new_page.frame.pack(side=tk.TOP, fill=tk.X)
        new_page.enter(old_page)

        self._update_buttons()
        self.progress_string.set("{0}/{1}".format(index + 1, len(self)))

    def _update_buttons(self):
        """
        Updates the state of the wizard buttons.
        :return:
        """
        self.previous_button.config(state=tk.DISABLED if self.current_page_index == 0 else tk.NORMAL)
        self.next_button.config(text="Submit" if self.current_page_index == (len(self) - 1) else "Next")


class WizardPage(object):
    """
    A single page in a wizard. All pages should be subclasses of this class.
    """

    def __init__(self, wizard):
        super(WizardPage, self).__init__()
        self.wizard = wizard
        self.frame = None
        wizard.add_page(self)

    @staticmethod
    def clear_children(widget):
        """
        Clears all children from the provided widget
        :return:
        """
        for widget in widget.winfo_children():
            widget.destroy()

    def enter(self, from_page):
        """
        Called when this page becomes active.
        :param from_page: The page the wizard was showing
        :return:
        """
        pass

    def leave(self, to_page):
        """
        Called when this page becomes inactive
        :param to_page: The page the wizard is about to show.
        :return:
        """
        return True