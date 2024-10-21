import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime


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
        # Пример данных
        data = {
            'passenger_id': ['P1', 'P1', 'P1', 'P2', 'P2', 'P3'],
            'flight_date': ['2024-01-15', '2024-02-20', '2024-02-25', '2024-03-15', '2024-04-20', '2024-04-25'],
            'from_coords': [(55, 37), (25, 55), (38, -77), (30, -90), (40, -74), (50, 30)],
            'to_coords': [(25, 55), (38, -77), (55, 37), (40, -74), (50, 30), (55, 37)]
        }
        df = pd.DataFrame(data)
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
        point_counter = 1
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
            color = month_colors[month]
            line_label = f'{month:02} {year}'

            showleg = True
            if month == previous_month:
                showleg = False
            previous_month = month

            # Линия полёта
            fig.add_trace(go.Scattergeo(
                locationmode='ISO-3',
                lon=[from_coord[1], to_coord[1]],
                lat=[from_coord[0], to_coord[0]],
                mode='lines',
                line=dict(width=4, color=color),
                name=line_label,
                showlegend=showleg
            ))

            # Добавление уникальных точек и их индексов
            if from_coord not in unique_points:
                unique_points[from_coord] = []
            if point_counter - 1 not in unique_points.get(from_coord, []):
                unique_points[from_coord].append(point_counter)
                point_counter += 1
            if to_coord not in unique_points:
                unique_points[to_coord] = []
            unique_points[to_coord].append(point_counter)
            point_counter += 1

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
                textfont=dict(size=16, color='black', family='Arial', weight='bold'),
                showlegend=False  # Убираем подписи вершин из легенды
            ))

        return fig

    # Функция для создания графа для всех пассажиров с заданными датами
    def generate_graphs(start_date, end_date, N):

        passenger_counts = df['passenger_id'].value_counts()
        filtered_passengers = passenger_counts[passenger_counts >= N].index

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
                projection_type='natural earth',
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
            html.Label("Начальная дата: "),
            dcc.DatePickerSingle(
                id='start-date-picker',
                date=datetime(2023, 1, 1),
                display_format='YYYY-MM-DD'
            ),
            html.Label("Конечная дата: "),
            dcc.DatePickerSingle(
                id='end-date-picker',
                date=datetime(2025, 1, 1),
                display_format='YYYY-MM-DD'
            ),
            html.Label("Минимальное количество полётов:"),
            dcc.Input(id='input-N', type='number', value=2, min=1),  # Поле ввода для N
            html.Button('Применить', id='apply-button', n_clicks=0)
        ]),
        dcc.Graph(id='flight-graph', style={'width': '90vw', 'height': '80vh'}),
    ])

    # Обновление графика на основе выбранных дат и нажатия кнопки
    @app.callback(
        Output('flight-graph', 'figure'),
        [Input('apply-button', 'n_clicks')],
        [State('start-date-picker', 'date'),
         State('end-date-picker', 'date'),
         State('input-N', 'value')]  # Получаем значение N из поля ввода
    )
    def update_graph(n_clicks, start_date, end_date, N):
        # Преобразование дат в формат datetime
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

        return generate_graphs(start_date, end_date, N)

    # Запуск приложения
    app.run_server(debug=True)
