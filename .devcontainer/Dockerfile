ARG UV_VERSION
FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv_source

FROM ubuntu:noble

ARG PYTHON_VERSION
RUN --mount=type=cache,target=/var/cache/apt \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
    wget \
    unzip \
    curl \
    git-core \
    vim \
    htop \
    zsh \
    sudo \
    python${PYTHON_VERSION} \
    python${PYTHON_VERSION}-dev \
    python3-setuptools \
    ca-certificates \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/tmp/* /var/lib/apt/lists/*

RUN rm /usr/lib/python${PYTHON_VERSION}/EXTERNALLY-MANAGED
RUN update-alternatives --install /usr/bin/python python /usr/bin/python${PYTHON_VERSION} 1 \
    && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python${PYTHON_VERSION} 1

COPY --from=uv_source /uv /uvx /bin/

WORKDIR /tmp

COPY .devcontainer/requirements.txt .
RUN uv pip install -r requirements.txt --system

COPY ./requirements.txt requirements.txt
RUN uv pip install -r requirements.txt --system

RUN rm -rf /tmp/*

ARG DEVCONTAINER_USER
RUN usermod -aG sudo $DEVCONTAINER_USER \
    && passwd -d $DEVCONTAINER_USER

ENV SHELL=/usr/bin/zsh
RUN chsh $DEVCONTAINER_USER -s $SHELL

WORKDIR /workspace

USER $DEVCONTAINER_USER
RUN sh -c "$(curl -fsSL https://raw.github.com/ohmyzsh/ohmyzsh/master/tools/install.sh)"
