import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime


def get_dataframe_for_work(data, airports):

    result = data[['ID','From','To','Date']]
    result['from_lat'] = result['From'].map(airports.set_index('iata_code')['latitude'])
    result['from_lon'] = result['From'].map(airports.set_index('iata_code')['longitude'])
    result['to_lat'] = result['To'].map(airports.set_index('iata_code')['latitude'])
    result['to_lon'] = result['To'].map(airports.set_index('iata_code')['longitude'])

    result['from_coords'] = list(zip(result['from_lat'], result['from_lon']))
    result['to_coords'] = list(zip(result['to_lat'], result['to_lon']))

    result.rename(columns={'ID': 'passenger_id', 'Date': 'flight_date'}, inplace=True)

    result['flight_date'] = pd.to_datetime(result['flight_date'])
    result = result.sort_values(by='flight_date')

    return result[['passenger_id','flight_date','from_coords','to_coords']]

def create_flight_graph_app(df):
    """
    Функция для создания и запуска Dash приложения, визуализирующего полёты пассажиров на карте.

    Параметры:
    df (pandas.DataFrame): DataFrame с информацией о полетах. Cтолбцы:
        - 'passenger_id': идентификатор пассажира - строка,
        - 'flight_date': дата полета - в формате datetime,
        - 'from_coords': координаты отправления (широта, долгота),
        - 'to_coords': координаты прибытия (широта, долгота).

    # Пример использования:
        # Пример 1
        data = {
            'passenger_id': ['P1', 'P1', 'P1', 'P2', 'P2', 'P3'],
            'flight_date': ['2024-01-15', '2024-02-20', '2024-02-25', '2024-03-15', '2024-04-20', '2024-04-25'],
            'from_coords': [(55, 37), (25, 55), (38, -77), (30, -90), (40, -74), (50, 30)],
            'to_coords': [(25, 55), (38, -77), (55, 37), (40, -74), (50, 30), (55, 37)]
        }
        df = pd.DataFrame(data)
        df['flight_date'] = pd.to_datetime(df['flight_date'])
        create_flight_graph_app(df)
        
        # Пример 2
        filepath='/home/alsmr/Загрузки/from_to_with_coords.csv'
        def parse_coords(coord_str):
            return tuple(map(float, coord_str.strip('()').split(', ')))
        df = pd.read_csv(filepath,
                         converters={'from_coords': parse_coords,'to_coords': parse_coords})
        df['flight_date'] = pd.to_datetime(df['flight_date'])
        create_flight_graph_app(df)
    """

    # Создаем приложение Dash
    app = dash.Dash(__name__)

    # Функция для построения графа полётов для одного пассажира
    def create_flight_graph(df, passenger_id, start_date, end_date):
        fig = go.Figure()

        month_colors = {1: 'red', 2: 'orange', 3: 'yellow', 4: 'green',
                        5: 'blue', 6: 'indigo', 7: 'violet', 8: 'purple',
                        9: 'brown', 10: 'pink', 11: 'grey', 12: 'black'}

        unique_points = {}
        point_counter = 0
        previous_month = None  # Для отслеживания предыдущего месяца

        # Фильтруем полёты по датам
        passenger_flights = df[(df['passenger_id'] == passenger_id) &
                               (df['flight_date'] >= start_date) &
                               (df['flight_date'] <= end_date)]

        for index, row in passenger_flights.iterrows():
            from_coord = row['from_coords']
            to_coord = row['to_coords']
            flight_date = row['flight_date']
            month = pd.to_datetime(flight_date).month
            year = pd.to_datetime(flight_date).year
            day = pd.to_datetime(flight_date).day
            color = month_colors[month]

            # Добавление уникальных точек и их индексов
            if from_coord not in unique_points:
                unique_points[from_coord] = []
            if point_counter - 1 not in unique_points.get(from_coord, []):
                unique_points[from_coord].append(point_counter + 1)
                point_counter += 2
            if to_coord not in unique_points:
                unique_points[to_coord] = []
            unique_points[to_coord].append(point_counter)
            point_counter += 1

            line_label = f'{day:02}/{month:02} {year}:   {point_counter-2} --> {point_counter-1}'
            showleg = True
            #if month == previous_month:
            #    showleg = False
            #previous_month = month
            # Линия полёта
            fig.add_trace(go.Scattergeo(
                locationmode='ISO-3',
                lon=[from_coord[1], to_coord[1]],
                lat=[from_coord[0], to_coord[0]],
                mode='lines',
                line=dict(width=3, color=color),
                name=line_label,
                showlegend=showleg
            ))

        # Отображение вершин с индексами
        for coord, point_numbers in unique_points.items():
            point_numbers_str = ', '.join(map(str, point_numbers))
            fig.add_trace(go.Scattergeo(
                lon=[coord[1]],
                lat=[coord[0]],
                mode='markers+text',
                marker=dict(size=4, color='black', symbol='circle'),
                text=point_numbers_str,
                textposition='top center',
                textfont=dict(size=14, color='black', family='Arial', weight='bold'),
                showlegend=False  # Убираем подписи вершин из легенды
            ))

        return fig

    # Функция для создания графа для всех пассажиров с заданными датами
    def generate_graphs(start_date, end_date, Nmin, Nmax, projection_type):

        passenger_counts = df['passenger_id'].value_counts()
        filtered_passengers = passenger_counts[(passenger_counts >= Nmin) & (passenger_counts <= Nmax)].index

        figs = {passenger: create_flight_graph(df, passenger, start_date, end_date) for passenger in
                filtered_passengers}

        final_fig = go.Figure()

        # Добавляем все графы в один
        for fig in figs.values():
            final_fig.add_traces(fig.data)

        # Генерируем кнопки
        buttons = []
        trace_offset = 0

        for passenger in filtered_passengers:
            # Устанавливаем видимость данных пассажира
            visible_traces = [False] * len(final_fig.data)

            num_traces = len(figs[passenger].data)
            visible_traces[trace_offset:trace_offset + num_traces] = [True] * num_traces
            trace_offset += num_traces

            buttons.append(dict(
                args=[{'visible': visible_traces}],
                label=passenger,
                method='update'
            ))

        # Меню выбора пассажира
        final_fig.update_layout(
            updatemenus=[{
                'buttons': buttons,
                'direction': 'down',
                'showactive': True
            }],
            geo=dict(
                projection_type=projection_type,
                showcoastlines=True,
                coastlinecolor="black",
                showland=True,
                landcolor="lightgray",
                showocean=True,
                oceancolor="lightblue",
                countrycolor="black",
            )
        )

        # Отображение первой фигуры по умолчанию
        for i, trace in enumerate(final_fig.data):
            trace.visible = (i < len(figs[filtered_passengers[0]].data))  # Показываем только граф первого пассажира

        return final_fig

    # Макет приложения
    app.layout = html.Div([
        html.H1("Графы полётов пассажиров"),
        html.Div([
            html.Label("Полёты с "),
            dcc.DatePickerSingle(
                id='start-date-picker',
                date=datetime(2015, 1, 1),
                display_format='YYYY-MM-DD'
            ),
            html.Label("\tдо "),
            dcc.DatePickerSingle(
                id='end-date-picker',
                date=datetime(2025, 1, 1),
                display_format='YYYY-MM-DD',
                style={'margin-right': '10px'}
            ),
            html.Label("  Мин. кол-во полётов:"),
            dcc.Input(id='input-Nmin', type='number', value=12, min=1, style={'width': '50px', 'margin-right': '10px'}),
            html.Label("  Макс. кол-во полётов:"),
            dcc.Input(id='input-Nmax', type='number', value=15, min=1, style={'width': '50px', 'margin-right': '10px'}),
            html.Label("\t\t\tТип проекции: "),
            dcc.Dropdown(
                id='projection-type',
                options=[
                    {'label': '2D (Natural Earth)', 'value': 'natural earth'},
                    {'label': '3D (Orthographic)', 'value': 'orthographic'}
                ],
                value='natural earth',  # Значение по умолчанию
                style={'width': '170px', 'display': 'inline-block'},
                clearable=False
            ),
            html.Button('Применить', id='apply-button', n_clicks=0, style={'margin-left': '10px'}),
            ], style={'display': 'flex', 'align-items': 'center', 'margin-top': '0px', 'margin-bottom': '0px'}),
        dcc.Graph(id='flight-graph', style={'width': '90vw', 'height': '80vh'}),
    ])

    # Обновление графика на основе выбранных дат и нажатия кнопки
    @app.callback(
        Output('flight-graph', 'figure'),
        [Input('apply-button', 'n_clicks')],
        [State('start-date-picker', 'date'),
         State('end-date-picker', 'date'),
         State('input-Nmin', 'value'),
         State('input-Nmax', 'value'),
         State('projection-type', 'value')]
    )

    def update_graph(n_clicks, start_date, end_date, Nmin, Nmax, type):
        # Преобразование дат в формат datetime
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

        return generate_graphs(start_date, end_date, Nmin, Nmax, type)

    # Запуск приложения
    app.run_server(host="0.0.0.0", debug=False)

