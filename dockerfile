# --- stage 1: go, ,libavif, libvips setup ---
FROM amazonlinux:2023 AS builder

# ARG TARGETARCH arm64 (recommanded)
ARG GO_VERSION=1.24.5
ARG VIPS_VERSION=8.17.1
ARG LIBGAVIF_VERSION=1.3.0
ARG AOM_VERSION=3.10.0
ARG LIBHEIF_VERSION=1.20.1
ARG LIBDE265_VERSION=1.0.16
ARG SVT_AV1_VERSION=3.1.0 
ARG DAV1D_VERSION=1.5.1 
ARG TARGETARCH

# dependency of go, libavif, libvips
RUN dnf install -y --allowerasing \
    gcc \
    gcc-c++ \
    make \
    pkg-config \
    cmake \
    git \
    nasm \
    # libheif dependencies
    libtool \
    automake \
    autoconf \
    # libvips dependencies
    glib2-devel \
    expat-devel \
    gobject-introspection-devel \
    libjpeg-turbo-devel \
    libpng-devel \
    giflib-devel \
    libexif-devel \
    librsvg2-devel \
    libtiff-devel \
    libwebp-devel \
    pango-devel \
    cairo-gobject-devel \
    xz \
    gzip \
    meson \
    ninja-build && \
    dnf clean all

ENV GOPATH=/go
ENV VIPS_PREFIX=/opt/vips
ENV PATH=${VIPS_PREFIX}/bin:${GOPATH}/bin:/usr/local/go/bin:${PATH}
ENV PKG_CONFIG_PATH=${VIPS_PREFIX}/lib64/pkgconfig:${VIPS_PREFIX}/lib/pkgconfig
ENV LD_LIBRARY_PATH=${VIPS_PREFIX}/lib64:${VIPS_PREFIX}/lib


# Go 
RUN dnf install -y --allowerasing curl tar gzip && \
    curl -L "https://golang.org/dl/go${GO_VERSION}.linux-${TARGETARCH}.tar.gz" | tar -C /usr/local -zxf -

# libaom 
RUN git clone --branch v${AOM_VERSION} --depth 1 https://aomedia.googlesource.com/aom && \
    cd aom && \
    mkdir -p build && \
    cd build && \
    cmake .. -G "Ninja" -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON -DCMAKE_INSTALL_PREFIX=${VIPS_PREFIX} -DENABLE_DOCS=OFF -DENABLE_TESTS=OFF && \
    ninja && ninja install

# SVT-AV1
RUN git clone --branch v${SVT_AV1_VERSION} --depth 1 https://gitlab.com/AOMediaCodec/SVT-AV1.git && \
    cd SVT-AV1/Build && \
    cmake .. -G "Ninja" -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON -DCMAKE_INSTALL_PREFIX=${VIPS_PREFIX} && \
    ninja && \
    ninja install && \
    ldconfig

# dav1d
RUN git clone --branch ${DAV1D_VERSION} --depth 1 https://code.videolan.org/videolan/dav1d.git && \
    cd dav1d && \
    mkdir build && \
    meson setup build --prefix=${VIPS_PREFIX} --buildtype=release --default-library=shared --libdir=lib && \
    ninja -C build && \
    ninja -C build install && \
    ldconfig


# libavif 
RUN git clone --branch v${LIBGAVIF_VERSION} --depth 1 https://github.com/AOMediaCodec/libavif.git && \
    cd libavif && \
    mkdir build && \
    cmake -S . -B build \
    -G "Ninja" \
    -DCMAKE_INSTALL_PREFIX=${VIPS_PREFIX} \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_SHARED_LIBS=ON \
    -DCMAKE_INSTALL_PREFIX=/opt/vips \
    -DAVIF_CODEC_AOM=SYSTEM \
    -DAVIF_CODEC_SVT=SYSTEM \
    -DAVIF_CODEC_DAV1D=SYSTEM \
    -DAVIF_LIBYUV=LOCAL \
    -DAVIF_JPEG=LOCAL \
    -DAVIF_ZLIBPNG=LOCAL \
    -DCMAKE_PREFIX_PATH=${VIPS_PREFIX} && \
    ninja -C build && \
    ninja -C build install && \
    ldconfig

# libde265 (dependency for libheif HEIC support)
RUN git clone --branch v${LIBDE265_VERSION} --depth 1 https://github.com/strukturag/libde265.git && \
    cd libde265 && \
    mkdir build && \
    cmake -S . -B build \
    -G "Ninja" \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_SHARED_LIBS=ON \
    -DCMAKE_INSTALL_PREFIX=${VIPS_PREFIX} && \
    ninja -C build && \
    ninja -C build install && \
    ldconfig


# libheif (dependency for libvips) 
RUN git clone --branch v${LIBHEIF_VERSION} --depth 1 https://github.com/strukturag/libheif.git && \
    cd libheif && \
    mkdir build && \
    cmake -S . -B build \
    -G "Ninja" \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_SHARED_LIBS=ON \
    -DCMAKE_INSTALL_PREFIX=${VIPS_PREFIX} \
    -DCMAKE_INSTALL_LIBDIR=lib64 \
    -DCMAKE_PREFIX_PATH=${VIPS_PREFIX} \
    -DWITH_LIBDE265=ON \
    -DWITH_AVIF=ON \
    -DWITH_AOM_ENCODER=ON \
    -DWITH_AOM_DECODER=ON \
    -DWITH_SvtEnc=ON \
    -DWITH_DAV1D=ON \
    -DWITH_JPEG_DECODER=ON \
    -DWITH_JPEG_ENCODER=ON \
    -DWITH_LIBSHARPYUV=ON \
    -DENABLE_PLUGIN_LOADING=OFF && \
    ninja -C build && \
    ninja -C build install && \
    ldconfig 


# libvips 
RUN curl -L https://github.com/libvips/libvips/releases/download/v${VIPS_VERSION}/vips-${VIPS_VERSION}.tar.xz | tar -xJvf - && \
    cd vips-${VIPS_VERSION} && \
    PKG_CONFIG_PATH="${VIPS_PREFIX}/lib/pkgconfig:${VIPS_PREFIX}/lib64/pkgconfig" \
    meson setup build --prefix=${VIPS_PREFIX} --buildtype=release --default-library=shared \
    -Dheif=enabled \
    -Djpeg=enabled \
    -Dpng=enabled \
    -Dtiff=enabled \
    -Dwebp=enabled \
    -Dintrospection=disabled \
    -Dmagick=disabled \
    -Dpdf=disabled \
    -Dsvg=disabled \
    -Dgif=disabled \
    -Dfuzzing=disabled && \
    ninja -C build -j$(nproc) && \
    ninja -C build install && \
    ldconfig 

# Go build
WORKDIR /app

# Go module dependency download
COPY go.mod go.sum ./
RUN go mod tidy

COPY . .

# Go build
RUN CGO_ENABLED=1 go build -ldflags="-s -w" -tags vips_full -o main . 

# --- stage 2: lambda setup ---
FROM public.ecr.aws/lambda/provided:al2023

# mininal lib
RUN dnf install -y \
    glib2 \
    expat \
    libjpeg-turbo \
    libpng \
    libtiff \
    libwebp \
    libexif \
    pango \
    librsvg2 \
    cairo-gobject && \
    dnf clean all

# get lib from builder
COPY --from=builder /opt/vips /opt/vips

# lib path for system
ENV LD_LIBRARY_PATH=/opt/vips/lib:/opt/vips/lib64

# run go
COPY --from=builder /app/main /var/runtime/bootstrap

# Lambda
CMD [ "bootstrap" ]