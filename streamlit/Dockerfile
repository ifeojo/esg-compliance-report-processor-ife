#force change
FROM python:3.12

RUN apt-get update && apt-get install -y curl

RUN useradd -m streamlituser

EXPOSE 8501
WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

RUN chown -R streamlituser:streamlituser /app

USER streamlituser

HEALTHCHECK CMD curl --fail http://localhost:8501/ || exit 1

CMD streamlit run Welcome.py