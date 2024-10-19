import plotly.graph_objects as go



def print_map(from_coords, to_coords, flight_dates):
    """
    Функция для визуализации графа полётов на карте.

    Аргументы:
    from_coords -- список кортежей с координатами мест отправления (широта, долгота)
    to_coords -- список кортежей с координатами мест назначения (широта, долгота)
    flight_dates -- список строк с датами полётов в формате 'YYYY-MM-DD'

    Функция строит карту с линиями, представляющими полёты между точками,
    а также добавляет индексы вершинам, соответствующие каждому уникальному её посещению.

    # Пример использования:
        from_coords = [(55, 37), (25, 55), (38, -77), (55, 37)]
        to_coords = [(25, 55), (38, -77), (55, 37), (-20, 45)]
        flight_dates = ['2024-01-15', '2024-02-20', '2024-03-25', '2024-03-26']
        print_map(from_coords, to_coords, flight_dates)
    """

    fig = go.Figure()

    # Цвета для обозначения месяца перелёта
    month_colors = {1: 'red', 2: 'orange', 3: 'yellow', 4: 'green',
                    5: 'blue', 6: 'indigo', 7: 'violet', 8: 'purple',
                    9: 'brown', 10: 'pink', 11: 'grey', 12: 'black'}
    unique_points = {}
    point_counter = 1

    for index, (from_coord, to_coord, flight_date) in enumerate(zip(from_coords, to_coords, flight_dates)):
        month = int(flight_date.split('-')[1])  # Получаем месяц из даты
        color = month_colors[month]
        line_label = f'{flight_date}'
        # Изображение линий
        fig.add_trace(go.Scattergeo(
            locationmode='ISO-3',
            lon=[from_coord[1], to_coord[1]],
            lat=[from_coord[0], to_coord[0]],
            mode='lines',
            line=dict(width=4, color=color),
            name=line_label
        ))

        # Добавление индексов вершинам
        if from_coord not in unique_points:
            unique_points[from_coord] = []
        if point_counter - 1 not in unique_points.get(from_coord, []):
            unique_points[from_coord].append(point_counter)
            point_counter += 1
        if to_coord not in unique_points:
            unique_points[to_coord] = []
        unique_points[to_coord].append(point_counter)
        point_counter += 1

    # Изображение вершин
    for coord, point_numbers in unique_points.items():
        # Преобразуем список индексов в строку для отображения
        point_numbers_str = ', '.join(map(str, point_numbers))
        fig.add_trace(go.Scattergeo(
            lon=[coord[1]],
            lat=[coord[0]],
            mode='markers+text',
            marker=dict(size=4, color='black', symbol='circle'),
            text=point_numbers_str,
            textposition='top center',
            textfont=dict(size=16, color='black', family='Arial', weight='bold'),
            name=f'Point {point_numbers_str}'
        ))

    # Визуализация на карте
    fig.update_layout(
        title_text='Граф полётов',
        title_font=dict(size=24, family='Arial', weight='bold'),
        showlegend=True,
        geo=dict(
            projection_type='natural earth',  # 'orthographic' - 3D
            showcoastlines=True,
            coastlinecolor="black",
            showland=True,
            landcolor="lightgray",
            showocean=True,
            oceancolor="lightblue",
            countrycolor="black",
        ),
    )

    fig.show()
