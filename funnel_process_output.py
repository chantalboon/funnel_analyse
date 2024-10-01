import xlsxwriter
from xlsxwriter.utility import xl_range
import pandas as pd

class FunnelProcessOutput:

    def __init__(self, settings: dict, data):
        self.settings = settings
        # self.scope = settings['definitions']['funnel_scope']
        # self.type = settings['definitions']['funnel_type']
        # self.funnel_dimensions = settings['definitions']['breakdown_dimension']
        # self.date_from, self.date_to = settings['definitions']['date_range']
        self.sheet = 'dummy'
        self.row = 1
        self.col = 1
        self.cnt_row = 1
        self.cnt_col = 6
        self.skip_rows = 30
        self.header = None
        self.bold = None
        self.table_cell = None
        self.table_header = None
        self.max_step = data["stepnumber"].iloc[-1]
        self.workbook = None
        self.worksheet = None

    def write_output_excel(self, data):
        writer = self.create_workbook()

        self.write_sheet_results(data)

        self.write_sheet_definitions()

        # Close the Pandas Excel writer and output the Excel file.
        writer.save()

    def create_workbook(self):
        # Create a Pandas Excel writer using XlsxWriter as the engine.
        writer = pd.ExcelWriter('Funnel_analyse.xlsx', engine='xlsxwriter')
        self.workbook = writer.book
        self.workbook_formatting()
        return writer

    def workbook_formatting(self):
        self.header = self.workbook.add_format({
            'bold': True,
            'font_size': 18,
            'font_color': '#FFFFFF',
            'valign': 'top',
            'fg_color': '#003D86',
            'border': 1})
        self.bold = self.workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'font_color': '#003D86',
            'valign': 'top',
            'border': None})
        self.table_cell = self.workbook.add_format({
            'text_wrap': True,
            'align': 'right',
            'valign': 'top',
            'font_color': '#003D86',
            'border_color': '#003D86',
            'border': 1})
        self.table_header = self.workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'align': 'right',
            'valign': 'top',
            'font_color': '#003D86',
            'border_color': '#003D86',
            'border': 1})
        self.sql_code = self.workbook.add_format({
            'font_name': 'courier',
            'font_size': 9,
            'text_wrap': True,
            'shrink': True
        })

    def create_worksheet(self):
        self.worksheet = self.workbook.add_worksheet(name=self.sheet)
        self.worksheet.hide_gridlines(2)
        self.worksheet.set_column("B:F", 20)
        self.row = 1

    def write_sheet_results(self, data):
        self.sheet = 'results'
        self.create_worksheet()

        self.write_summary('Summary')

        # transform results to pivot
        funnel_pivot = data.pivot(index=['breakdown', 'group'], columns='stepnumber', values='id').reset_index()

        # split on breakdown value
        breakdown = data['breakdown'].unique()
        for br in breakdown:
            self.write_breakdown_results(br, funnel_pivot)

    def write_sheet_definitions(self):
        self.sheet = 'definitions'
        self.create_worksheet()

        self.write_data_row('Scope', self.settings['definitions']['funnel_scope'])
        self.write_data_row('Type', self.settings['definitions']['funnel_type'])
        self.write_data_row('Date range', self.settings['definitions']['date_range'])

        for dim in self.settings['definitions']['breakdown_dimension']:
            self.write_data_row('Breakdown dimensions', dim)

        self.write_data_row('Filter', None)
        for f in self.settings['definitions']['filters']:
            self.write_data_row(None, f)

        self.write_data_row('Steps', None)
        for s in self.settings['definitions']['steps']:
            self.write_data_row(s['stepnumber'], None)
            for f in s['step']:
                self.write_data_row(None, f)

        self.write_data_row('Steps SQL', None)

        for s in self.settings['prepped']['steps']:
            self.write_data_sql(s)

    def write_data_sql(self, s):
        cell_range = xl_range(self.row, self.col + 1, self.row, self.col + 4)
        self.worksheet.set_row(self.row, 400)
        self.worksheet.merge_range(cell_range, s['step_sql'], self.sql_code)
        self.row += 1

    def write_data_row(self, header, values):
        self.worksheet.write(self.row, 1, header, self.bold)
        col = 2
        if type(values) is str:
            self.worksheet.write(self.row, col, values)
        if type(values) is dict:
            for value in values.values():
                self.worksheet.write(self.row, col, value)
                col += 1
        if type(values) is list:
            for value in values:
                self.worksheet.write(self.row, col, value)
                col += 1
        self.row += 1
        return

    def write_summary(self, title):
        self.write_header(title, self.row)
        self.write_data_row('Funnel scope', self.settings['definitions']['funnel_scope'])
        self.write_data_row('Funnel type', self.settings['definitions']['funnel_type'])
        self.write_data_row('Date range', self.settings['definitions']['date_range'])
        self.row += 2

    def write_header(self, value, row):
        cell_range = xl_range(row, self.col, row, self.col + self.cnt_col - 1)
        self.worksheet.merge_range(cell_range, value, self.header)
        self.row += 1

    def create_stacked_bar_chart(self, br):
        # Create a chart object.
        chart = self.workbook.add_chart({'type': 'column', 'subtype': 'stacked'})
        self.set_chart_options(br, chart, self.cnt_col)
        chart_row = self.row - 1
        # Configure the series of the chart from the dataframe data.
        for i in range(1, self.cnt_row + 1):
            chart.add_series({
                'name': ['results', chart_row + i, self.col],
                'categories': ['results', chart_row, self.col + 1, chart_row, self.col + self.cnt_col - 1],
                'values': ['results', chart_row + i, self.col + 1, chart_row + i, self.col + self.cnt_col - 1],
                'data_labels': {'value': True},
                'fill': {'color': '#003D86', 'transparency': (i-1) * 50},
            })
        # Insert the chart into the worksheet.
        self.worksheet.insert_chart(self.row + self.cnt_row + 8, self.col + 1, chart)

    def set_chart_options(self, br, chart, cnt_col):
        # chart.set_x_axis({'name': 'Step'})
        chart.set_y_axis({'major_gridlines': {'visible': False}, 'visible': False})
        if br != '-':
            chart.set_title({'name': br})
        chart.set_legend({'position': 'bottom'})
        chart.set_size({'width': (cnt_col - 1) * 145, 'height': 300})
        chart.set_plotarea({'fill': {'none': True}})

    def write_breakdown_results(self, br, data):
        df = data[data['breakdown'] == br].copy().reset_index()
        df = df.fillna(0.0).drop(columns=['breakdown', 'index'])
        self.write_table(df, br)
        self.create_stacked_bar_chart(br)
        self.row = self.row + self.cnt_row + 25

    def write_table(self, df, breakdown):
        # Get the dimensions of the dataframe.
        (self.cnt_row, self.cnt_col) = df.shape

        if breakdown != '-':
            self.write_header(breakdown, self.row - 1)

        self.write_table_header()
        self.write_table_content(df)
        self.write_table_footer(df)

    def write_table_content(self, df):
        table_col = self.col
        for c in df.columns:
            self.write_table_cell(c, 1, table_col, self.table_header)
            df_col = df[c]
            for i, value in df_col.items():
                self.write_table_cell(value, i + 2, table_col, self.table_cell)
            table_col += 1
        self.row += i + 1

    def write_table_cell(self, value, row_index, col_index, cell_format):
        if not(value == '0'):
            self.worksheet.write(self.row + row_index, col_index, value, cell_format)

    def write_table_header(self):
        cell_range = xl_range(self.row, self.col + 1, self.row, self.col + self.cnt_col - 1)
        self.worksheet.merge_range(cell_range, 'Steps', self.table_header)

    def write_table_footer(self, df):
        self.row += self.cnt_row
        total_start = df.iloc[:, 1].sum()

        for i, c in enumerate(df.columns):
            if not (c == 'group'):
                total = int(df[c].sum())
                perc_start = f'''{round((total / total_start) * 100, 1)}%'''
                self.write_table_footer_row(1, i, 'Totals', total)
                self.write_table_footer_row(2, i, '% of start', perc_start)
                if not(c == self.max_step):
                    uitval = df[df['group'] == 'base'][c].item() - df[df['group'] == 'base'].iloc[:, i + 1].item()
                    next_step = total - uitval
                    perc_uitval = f'''{round((uitval/total)*100,1)}%'''
                    perc_next_step = f'''{round((next_step / total) * 100, 1)}%'''
                    self.write_table_footer_row(3, i, 'To next step', next_step)
                    self.write_table_footer_row(4, i, '% to next step', perc_next_step)
                    self.write_table_footer_row(5, i, 'Uitval', uitval)
                    self.write_table_footer_row(6, i, '% Uitval', perc_uitval)

    def write_table_footer_old(self, df):
        self.row += self.cnt_row
        total_start = df[1].sum()
        for i in range(1, self.max_step + 1):
            total = df[i].sum()
            perc_start = f'''{round((total / total_start) * 100, 1)}%'''
            self.write_table_footer_row(1, i, 'Totals', total)
            self.write_table_footer_row(2, i, '% of start', perc_start)
            if i < self.max_step:
                uitval = df[df['group'] == 'base'][i].item() - df[df['group'] == 'base'][i + 1].item()
                next_step = total - uitval
                perc_uitval = f'''{round((uitval / total) * 100, 1)}%'''
                perc_next_step = f'''{round((next_step / total) * 100, 1)}%'''
                self.write_table_footer_row(3, i, 'To next step', next_step)
                self.write_table_footer_row(4, i, '% to next step', perc_next_step)
                self.write_table_footer_row(5, i, 'Uitval', uitval)
                self.write_table_footer_row(6, i, '% Uitval', perc_uitval)

    def write_table_footer_row(self, row_index, step, title, value):
        self.worksheet.write(self.row + row_index, self.col, title, self.table_header)
        self.worksheet.write(self.row + row_index, self.col + step, value, self.table_cell)
