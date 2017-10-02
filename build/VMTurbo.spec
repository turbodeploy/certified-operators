Name:       VMTurbo
Version:    %vers
Release:    %svn_revision
Summary:    VMTurbo
Group:      System/Management
License:    Commercial
URL:        http://vmturbo.com
Requires:   curl,docker>=1.10.0
BuildArch:  noarch
Vendor:     VMTurbo, Inc.
Packager:   jonathan.hosmer@vmturbo.com
Provides:   %{name}

%description

VMTurbo XL

%install
mkdir -p %{buildroot}%{_prefix}/vmturbo
cp -p docker-compose.yml %{buildroot}%{_prefix}/vmturbo

mkdir -p "%{buildroot}%{_sysconfdir}/docker/certs.d/registry:5000"
cp -p ca.crt "%{buildroot}%{_sysconfdir}/docker/certs.d/registry:5000"

%post
if [ $1 -eq 1 ]; then
    curl --create-dirs -sSLo \
        %{_prefix}/vmturbo/bin/docker-compose \
        https://github.com/docker/compose/releases/download/1.7.1/docker-compose-`uname -s`-`uname -m`
    ln -s %{_prefix}/vmturbo/bin/docker-compose %{_bindir}/docker-compose
    chmod 755 %{_prefix}/vmturbo/bin/docker-compose %{_bindir}/docker-compose
fi

%files
%defattr(644,root,root)
%{_prefix}/vmturbo/docker-compose.yml
%config(noreplace) %{_sysconfdir}/docker/certs.d/registry:5000/ca.crt

%changelog
* Thu May 5 2016 created source package
- jonathan.hosmer@vmturbo.com
