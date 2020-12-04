FROM almondsh/almond:latest

USER root
RUN apt-get update && apt-get install -y graphviz
USER $NB_UID
RUN jupyter labextension install @jupyterlab/plotly-extension

COPY --chown=1000:100 notebooks/ $HOME

#docker run \
#		-it \
#		--rm \
#		-p 50070:50070 \
#		-p 8088:8088 \
#		-p 9870:9870 \
#		-p 9864:9864 \
#		-v ${PWD}/input:/input/ \
#		-v ${PWD}/output:/result/ \
#		--name=${container.name} \
#		${container.base}/${container.name}:latest